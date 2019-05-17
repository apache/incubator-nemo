/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.nemo.compiler.frontend.beam.transform;

import org.apache.beam.runners.core.*;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.state.TimeDomain;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.display.DisplayData;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.PaneInfo;
import org.apache.beam.sdk.util.WindowTracing;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.WindowingStrategy;
import org.apache.nemo.common.Pair;
import org.apache.nemo.common.ir.AbstractOutputCollector;
import org.apache.nemo.common.ir.OutputCollector;
import org.apache.nemo.common.punctuation.Watermark;
import org.joda.time.Instant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.util.*;

/**
 * Groups elements according to key and window.
 * @param <K> key type.
 * @param <InputT> input type.
 */
public final class GBKPartialTransform<K, InputT>
  extends AbstractDoFnTransform<KV<K, InputT>, KeyedWorkItem<K, InputT>, KV<K, Iterable<InputT>>> {
  private static final Logger LOG = LoggerFactory.getLogger(GBKPartialTransform.class.getName());

  private final SystemReduceFn reduceFn; //private final Map<K, List<WindowedValue<InputT>>> keyToValues;
  private transient InMemoryTimerInternalsFactory<K> inMemoryTimerInternalsFactory;
  private transient InMemoryStateInternalsFactory<K> inMemoryStateInternalsFactory;
  private Watermark prevOutputWatermark;
  private final Map<K, Watermark> keyAndWatermarkHoldMap;
  private final WindowingStrategy windowingStrategy;
  private Watermark inputWatermark;

  private final Map<K, List<WindowedValue<InputT>>> keyToValues;

  int numProcessedData = 0;

  private boolean dataReceived = false;

  private transient OutputCollector originOc;

  /**
   * GroupByKey constructor.
   */
  public GBKPartialTransform(final Map<TupleTag<?>, Coder<?>> outputCoders,
                             final TupleTag<KV<K, Iterable<InputT>>> mainOutputTag,
                             final WindowingStrategy<?, ?> windowingStrategy,
                             final PipelineOptions options,
                             final SystemReduceFn reduceFn,
                             final DisplayData displayData) {
    super(null, /* doFn */
      null, /* inputCoder */
      outputCoders,
      mainOutputTag,
      Collections.emptyList(),  /*  GBK does not have additional outputs */
      windowingStrategy,
      Collections.emptyMap(), /*  GBK does not have additional side inputs */
      options,
      displayData);
    //this.keyToValues = new HashMap<>();
    this.reduceFn = reduceFn;
    this.prevOutputWatermark = new Watermark(Long.MIN_VALUE);
    this.inputWatermark = new Watermark(Long.MIN_VALUE);
    this.keyAndWatermarkHoldMap = new HashMap<>();
    this.windowingStrategy = windowingStrategy;
    this.keyToValues = new HashMap<>();
  }

  /**
   * This creates a new DoFn that groups elements by key and window.
   * @param doFn original doFn.
   * @return GroupAlsoByWindowViaWindowSetNewDoFn
   */
  @Override
  protected DoFn wrapDoFn(final DoFn doFn) {
    final Map<K, StateAndTimerForKey> map = new HashMap<>();
    this.inMemoryStateInternalsFactory = new InMemoryStateInternalsFactory<>();
    this.inMemoryTimerInternalsFactory = new InMemoryTimerInternalsFactory<>();


    // This function performs group by key and window operation
    return
      GroupAlsoByWindowViaWindowSetNewDoFn.create(
        getWindowingStrategy(),
        inMemoryStateInternalsFactory,
        inMemoryTimerInternalsFactory,
        null, // GBK has no sideinput.
        reduceFn,
        getOutputManager(),
        getMainOutputTag());
  }

  @Override
  OutputCollector wrapOutputCollector(final OutputCollector oc) {
    originOc = oc;
    return new GBKWOutputCollector(oc);
  }

  /**
   * It collects data for each key.
   * The collected data are emitted at {@link GBKPartialTransform#onWatermark(Watermark)}
   * @param element data element
   */
  @Override
  public void onData(final WindowedValue<KV<K, InputT>> element) {
    dataReceived = true;

    // drop late data
    if (element.getTimestamp().isAfter(inputWatermark.getTimestamp())) {
      // We can call Beam's DoFnRunner#processElement here,
      // but it may generate some overheads if we call the method for each data.
      // The `processElement` requires a `Iterator` of data, so we emit the buffered data every watermark.
      // TODO #250: But, this approach can delay the event processing in streaming,
      // TODO #250: if the watermark is not triggered for a long time.

      final KV<K, InputT> kv = element.getValue();

      //LOG.info("Handle element {}", element);

      checkAndInvokeBundle();
      final KeyedWorkItem<K, InputT> keyedWorkItem =
        KeyedWorkItems.elementsWorkItem(kv.getKey(),
          Collections.singletonList(element.withValue(kv.getValue())));
      numProcessedData += 1;
      // The DoFnRunner interface requires WindowedValue,
      // but this windowed value is actually not used in the ReduceFnRunner internal.
      getDoFnRunner().processElement(WindowedValue.valueInGlobalWindow(keyedWorkItem));
      checkAndFinishBundle();
    }
  }

  /**
   * Process the collected data and trigger timers.
   * @param processingTime processing time
   * @param synchronizedTime synchronized time
   */
  private void processElementsAndTriggerTimers(final Instant processingTime,
                                               final Instant synchronizedTime,
                                               final Watermark triggerWatermark) {

    // Trigger timers

    final int triggeredKeys = triggerTimers(processingTime, synchronizedTime, triggerWatermark, false);
    final long triggerTime = System.currentTimeMillis();

//    LOG.info("{} time to elem: {} trigger: {} triggered: {} triggeredKey: {}", getContext().getIRVertex().getId(),
//      (e-st), (triggerTime - st), triggeredKeys > 0, triggeredKeys);
  }

  /**
   * Output watermark
   * = max(prev output watermark,
   *          min(input watermark, watermark holds)).
   */
  private void emitOutputWatermark() {
    // Find min watermark hold
    final Watermark minWatermarkHold = keyAndWatermarkHoldMap.isEmpty()
      ? new Watermark(dataReceived ? Long.MIN_VALUE : Long.MAX_VALUE)
      : Collections.min(keyAndWatermarkHoldMap.values());
    final Watermark outputWatermarkCandidate = new Watermark(
      Math.max(prevOutputWatermark.getTimestamp(),
        Math.min(minWatermarkHold.getTimestamp(), inputWatermark.getTimestamp())));

    if (LOG.isDebugEnabled()) {
      LOG.debug("Watermark hold: {}, "
        + "inputWatermark: {}, outputWatermark: {}", minWatermarkHold, inputWatermark, prevOutputWatermark);
    }


    if (outputWatermarkCandidate.getTimestamp() > prevOutputWatermark.getTimestamp()) {
      // progress!
      prevOutputWatermark = outputWatermarkCandidate;
      // emit watermark

      //LOG.info("Emit watermark at GBKW: {}", outputWatermarkCandidate);
      getOutputCollector().emitWatermark(outputWatermarkCandidate);
      // Remove minimum watermark holds
      if (minWatermarkHold.getTimestamp() == outputWatermarkCandidate.getTimestamp()) {
        keyAndWatermarkHoldMap.entrySet()
          .removeIf(entry -> entry.getValue().getTimestamp() == minWatermarkHold.getTimestamp());
      }
    }
  }

  @Override
  public void onWatermark(final Watermark watermark) {

    //LOG.info("Watermark at GBKW: {}", watermark);
    checkAndInvokeBundle();
    inputWatermark = watermark;

    final long st = System.currentTimeMillis();
    processElementsAndTriggerTimers(Instant.now(), Instant.now(), inputWatermark);
    // Emit watermark to downstream operators

    emitOutputWatermark();
    final long et1 = System.currentTimeMillis();
    checkAndFinishBundle();

    final long et = System.currentTimeMillis();
//    LOG.info("{}/{} latency {}, watermark: {}, emitOutputWatermarkTime: {}",
//      getContext().getIRVertex().getId(), Thread.currentThread().getId(), (et-st),
//      new Instant(watermark.getInputTimestamp()), (et - et1));
  }

  /**
   * This advances the input watermark and processing time to the timestamp max value
   * in order to emit all data.
   */
  @Override
  protected void beforeClose() {
    // Finish any pending windows by advancing the input watermark to infinity.
    inputWatermark = new Watermark(BoundedWindow.TIMESTAMP_MAX_VALUE.getMillis());
    processElementsAndTriggerTimers(BoundedWindow.TIMESTAMP_MAX_VALUE, BoundedWindow.TIMESTAMP_MAX_VALUE, inputWatermark);
  }

  @Override
  public void flush() {
    LOG.info("Flush in GBKPartial!");
    triggerTimers(BoundedWindow.TIMESTAMP_MAX_VALUE, BoundedWindow.TIMESTAMP_MAX_VALUE,
      new Watermark(BoundedWindow.TIMESTAMP_MAX_VALUE.getMillis()), true);
  }

  /**
   * Trigger times for current key.
   * When triggering, it emits the windowed data to downstream operators.
   * @param processingTime processing time
   * @param synchronizedTime synchronized time
   */
  private int triggerTimers(final Instant processingTime,
                            final Instant synchronizedTime,
                            final Watermark triggerWatermark,
                            final boolean isFlush) {

    inMemoryTimerInternalsFactory.inputWatermarkTime = new Instant(triggerWatermark.getTimestamp());
    inMemoryTimerInternalsFactory.processingTime = processingTime;
    inMemoryTimerInternalsFactory.synchronizedProcessingTime = synchronizedTime;

    final long st = System.currentTimeMillis();
    final List<Pair<K, TimerInternals.TimerData>> timers = getEligibleTimers();

//    LOG.info("{}/{} GetEligibleTimer time: {}", getContext().getIRVertex().getId(),
//      Thread.currentThread().getId(), (System.currentTimeMillis() - st));

    // TODO: send start event
    /*
    if (!timers.isEmpty()) {
      final WindowedValue<KV<K, Iterable<InputT>>> startEvent =
        WindowedValue.valueInGlobalWindow(
          KV.of((K) new GBKLambdaEvent(GBKLambdaEvent.Type.START, new Integer(timers.size())),
            Collections.emptyList()));
      getOutputCollector().emit(startEvent);
    }
    */
    // TODO: end

    for (final Pair<K, TimerInternals.TimerData> timer : timers) {
      final NemoTimerInternals timerInternals =
        inMemoryTimerInternalsFactory.timerInternalsMap.get(timer.left());
      timerInternals.setCurrentInputWatermarkTime(new Instant(triggerWatermark.getTimestamp()));
      timerInternals.setCurrentProcessingTime(processingTime);
      timerInternals.setCurrentSynchronizedProcessingTime(synchronizedTime);

      // Trigger timers and emit windowed data
      final KeyedWorkItem<K, InputT> timerWorkItem =
        KeyedWorkItems.timersWorkItem(timer.left(), Collections.singletonList(timer.right()));
      // The DoFnRunner interface requires WindowedValue,
      // but this windowed value is actually not used in the ReduceFnRunner internal.
      getDoFnRunner().processElement(WindowedValue.valueInGlobalWindow(timerWorkItem));

      if (isFlush) {
        timerInternals.setCurrentSynchronizedProcessingTime(new Instant(inputWatermark.getTimestamp()));
      }

      /*
      timerInternals.decrementRegisteredTimer();
      if (!timerInternals.hasTimer()) {
        LOG.info("Remove key: {}", timer.left());
        inMemoryTimerInternalsFactory.timerInternalsMap.remove(timer.left());
      }
      */
    }

    if (isFlush) {
      inMemoryTimerInternalsFactory.inputWatermarkTime = new Instant(inputWatermark.getTimestamp());
    }

    // TODO: send end event
    /*
    if (!timers.isEmpty()) {
      final WindowedValue<KV<K, Iterable<InputT>>> endEvent =
        WindowedValue.valueInGlobalWindow(
          KV.of((K) new GBKLambdaEvent(GBKLambdaEvent.Type.END, new Integer(timers.size())),
            Collections.emptyList()));
      getOutputCollector().emit(endEvent);
    }
    */

    return timers.size();
  }

  /**
   * Get timer data.
   */
  private List<Pair<K, TimerInternals.TimerData>> getEligibleTimers() {
    final List<Pair<K, TimerInternals.TimerData>> timerData = new LinkedList<>();

    while (true) {
      Pair<K, TimerInternals.TimerData> timer;
      boolean hasFired = false;

      while ((timer = inMemoryTimerInternalsFactory.removeNextEventTimer()) != null) {
        hasFired = true;
        timerData.add(timer);
      }

      while ((timer = inMemoryTimerInternalsFactory.removeNextProcessingTimer()) != null) {
        hasFired = true;
        timerData.add(timer);
      }
      while ((timer = inMemoryTimerInternalsFactory.removeNextSynchronizedProcessingTimer()) != null) {
        hasFired = true;
        timerData.add(timer);
      }
      if (!hasFired) {
        break;
      }
    }

    return timerData;
  }

  /**
   * This class wraps the output collector to track the watermark hold of each key.
   */
  final class GBKWOutputCollector extends AbstractOutputCollector<WindowedValue<KV<K, Iterable<InputT>>>> {
    private final OutputCollector<WindowedValue<KV<K, Iterable<InputT>>>> outputCollector;
    GBKWOutputCollector(final OutputCollector<WindowedValue<KV<K, Iterable<InputT>>>> outputCollector) {
      this.outputCollector = outputCollector;
    }

    @Override
    public void emit(final WindowedValue<KV<K, Iterable<InputT>>> output) {

      //LOG.info("Partial output: {}", output);
      // The watermark advances only in ON_TIME
      if (output.getPane().getTiming().equals(PaneInfo.Timing.ON_TIME)) {
        final K key = output.getValue().getKey();
        final NemoTimerInternals timerInternals = (NemoTimerInternals)
          inMemoryTimerInternalsFactory.timerInternalsForKey(key);
        keyAndWatermarkHoldMap.put(key,
          // adds the output timestamp to the watermark hold of each key
          // +1 to the output timestamp because if the window is [0-5000), the timestamp is 4999
          new Watermark(output.getTimestamp().getMillis() + 1));
        timerInternals.setCurrentOutputWatermarkTime(new Instant(output.getTimestamp().getMillis() + 1));
      }
      //LOG.info("Set input timsestamp: {}", output.getTimestamp().getMillis());
      originOc.setInputTimestamp(output.getTimestamp().getMillis());
      outputCollector.emit(output);
    }

    @Override
    public void emitWatermark(final Watermark watermark) {

      //LOG.info("Partial watermark emit: {}", new Instant(watermark.getTimestamp()));
      outputCollector.emitWatermark(watermark);
    }
    @Override
    public <T> void emit(final String dstVertexId, final T output) {
      outputCollector.emit(dstVertexId, output);
    }
  }
}
