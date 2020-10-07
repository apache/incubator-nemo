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
import org.apache.beam.sdk.state.State;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.display.DisplayData;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.PaneInfo;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.WindowingStrategy;
import org.apache.nemo.common.Pair;
import org.apache.nemo.common.ir.OutputCollector;
import org.apache.nemo.common.punctuation.Watermark;
import org.apache.nemo.compiler.frontend.beam.transform.coders.GBKFinalStateCoder;
import org.joda.time.Instant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.beam.sdk.transforms.DoFnSchemaInformation;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Groups elements according to key and window.
 * @param <K> key type.
 * @param <InputT> input type.
 * @param <OutputT> output type.
 */
public final class GBKFinalTransform<K, InputT, OutputT>
  extends AbstractDoFnTransform<KV<K, InputT>, KeyedWorkItem<K, InputT>, KV<K, OutputT>> implements StatefulTransform<GBKFinalState<K>> {
  private static final Logger LOG = LoggerFactory.getLogger(GBKFinalTransform.class.getName());
  private final SystemReduceFn reduceFn;
  private transient InMemoryTimerInternalsFactory<K> inMemoryTimerInternalsFactory;
  private transient InMemoryStateInternalsFactory<K> inMemoryStateInternalsFactory;
  private Watermark prevOutputWatermark;
  private final Map<K, Watermark> keyAndWatermarkHoldMap;
  private Watermark inputWatermark;
  int numProcessedData = 0;
  private transient OutputCollector originOc;
  private final Coder<K> keyCoder;
  private final Coder windowCoder;

  /**
   * GroupByKey constructor.
   */
  public GBKFinalTransform(final Coder<K> keyCoder,
                           final Map<TupleTag<?>, Coder<?>> outputCoders,
                           final TupleTag<KV<K, OutputT>> mainOutputTag,
                           final WindowingStrategy<?, ?> windowingStrategy,
                           final PipelineOptions options,
                           final SystemReduceFn reduceFn,
                           final DoFnSchemaInformation doFnSchemaInformation,
                           final DisplayData displayData) {
    super(null, /* doFn */
      null, /* inputCoder */
      outputCoders,
      mainOutputTag,
      Collections.emptyList(),  /*  GBK does not have additional outputs */
      windowingStrategy,
      Collections.emptyMap(), /*  GBK does not have additional side inputs */
      options,
      displayData,
      DoFnSchemaInformation.create(),
      Collections.<String, PCollectionView<?>>emptyMap()); /* GBK does not have side inputs */
    this.windowCoder = windowingStrategy.getWindowFn().windowCoder();
    this.keyCoder = keyCoder;
    this.reduceFn = reduceFn;
    this.prevOutputWatermark = new Watermark(Long.MIN_VALUE);
    this.inputWatermark = new Watermark(Long.MIN_VALUE);
    this.keyAndWatermarkHoldMap = new HashMap<>();
  }

  /**
   * This creates a new DoFn that groups elements by key and window.
   * @param doFn original doFn.
   * @return GroupAlsoByWindowViaWindowSetNewDoFn
   */
  @Override
  protected DoFn wrapDoFn(final DoFn doFn) {
    final Map<StateTag, Pair<State, Coder>> map = new ConcurrentHashMap<>();

    if (inMemoryStateInternalsFactory == null) {
      this.inMemoryStateInternalsFactory = new InMemoryStateInternalsFactory<>();
    } else {
      LOG.info("InMemoryStateInternalFactroy is already set");
    }

    if (inMemoryTimerInternalsFactory == null) {
      this.inMemoryTimerInternalsFactory = new InMemoryTimerInternalsFactory<>();
    } else {
      LOG.info("InMemoryTimerInternalsFactory is already set");
    }


    // This function performs group by key and window operation.
    return
      GroupAlsoByWindowViaWindowSetNewDoFn.create(
        getWindowingStrategy(),
        inMemoryStateInternalsFactory,
        inMemoryTimerInternalsFactory,
        null, // GBK does not have side input.
        reduceFn,
        getOutputManager(),
        getMainOutputTag());
  }

  /**
   * Wrapper function of output collector
   * @param oc the original outputCollector.
   * @return GBKWOutputCollector
   */
  @Override
  OutputCollector wrapOutputCollector(final OutputCollector oc) {
    originOc = oc;
    return new GBKWOutputCollector(oc);
  }

  /**
   * It collects data for each key.
   * The collected data are emitted at {@link GBKFinalTransform#onWatermark(Watermark)}
   * @param element input data element.
   */
  @Override
  public void onData(final WindowedValue<KV<K, InputT>> element) {
    // Drop late data when current inputWatermark > end of window + allowed Lateness
    if (!inputWatermark.equals(new Watermark(Long.MIN_VALUE))) {
      Instant ts = new Instant(inputWatermark.getTimestamp() - getWindowingStrategy().getAllowedLateness().getMillis());
      if (element.isSingleWindowedValue()) {
        WindowedValue.SingleWindowedValue value_singlewindow = (WindowedValue.SingleWindowedValue) element;
        if (ts.isAfter(value_singlewindow.getWindow().maxTimestamp())) {
          return;
        }
      }
      else {
        ArrayList<BoundedWindow> removed = new ArrayList<>();
        for (Iterator<? extends BoundedWindow> iter = element.getWindows().iterator(); iter.hasNext();) {
          BoundedWindow curr = iter.next();
          if (ts.isAfter(curr.maxTimestamp())) {
            removed.add(curr);
          }
        }
        element.getWindows().removeAll(removed);
      }
    }
    if (!element.getWindows().isEmpty()) {
      try {
        checkAndInvokeBundle();
        final KV<K, InputT> kv = element.getValue();
        final KeyedWorkItem<K, InputT> keyedWorkItem =
          KeyedWorkItems.elementsWorkItem(kv.getKey(),
            Collections.singletonList(element.withValue(kv.getValue())));
        numProcessedData += 1;
        getDoFnRunner().processElement(WindowedValue.valueInGlobalWindow(keyedWorkItem));
        checkAndFinishBundle();
      } catch (final Exception e) {
        e.printStackTrace();
        throw new RuntimeException("exception trigggered element " + element.toString());
      }
    }
  }

  /**
   * Process the collected data and trigger timers.
   * @param processingTime processing time
   * @param synchronizedTime synchronized time
   * @param triggerWatermark watermark
   */
  private void processElementsAndTriggerTimers(final Instant processingTime,
                                               final Instant synchronizedTime,
                                               final Watermark triggerWatermark) {

    triggerTimers(processingTime, synchronizedTime, triggerWatermark);
  }

  /**
   * Output watermark
   * = max(prev output watermark,
   *          min(input watermark, watermark holds)).
   */
  private void emitOutputWatermark() {
    // Find min watermark hold
    Watermark minWatermarkHold = keyAndWatermarkHoldMap.isEmpty()
      ? new Watermark(Long.MAX_VALUE) : Collections.min(keyAndWatermarkHoldMap.values());

    Watermark outputWatermarkCandidate = new Watermark(
      Math.max(prevOutputWatermark.getTimestamp(),
        Math.min(minWatermarkHold.getTimestamp(), inputWatermark.getTimestamp())));

    if (LOG.isDebugEnabled()) {
      LOG.debug("Watermark hold: {}, "
        + "inputWatermark: {}, outputWatermark: {}", minWatermarkHold, inputWatermark, prevOutputWatermark);
    }

    // keep going if the watermark increases
    if (outputWatermarkCandidate.getTimestamp() > prevOutputWatermark.getTimestamp()) {
      // progress!
      prevOutputWatermark = outputWatermarkCandidate;
      // emit watermark

      //LOG.info("Emit watermark at GBKW: {}", outputWatermarkCandidate);
      getOutputCollector().emitWatermark(outputWatermarkCandidate);
      // Remove minimum watermark holds
      if (minWatermarkHold.getTimestamp() == outputWatermarkCandidate.getTimestamp()) {
        final long minWatermarkTimestamp = minWatermarkHold.getTimestamp();
        keyAndWatermarkHoldMap.entrySet()
          .removeIf(entry -> entry.getValue().getTimestamp() == minWatermarkTimestamp);
      }
    }
  }

  /**
   * Trigger timers that need to be fired at watermark
   * @param watermark watermark
   */
  @Override
  public void onWatermark(final Watermark watermark) {

    if (watermark.getTimestamp() <= inputWatermark.getTimestamp()) {
      LOG.info("Input watermark {} is before the prev watermark: {}", new Instant(watermark.getTimestamp()),
        new Instant(inputWatermark.getTimestamp()));
      return;
    }

    checkAndInvokeBundle();
    inputWatermark = watermark;
    // Triggering timers
    try {
      processElementsAndTriggerTimers(Instant.now(), Instant.now(), inputWatermark);
    } catch (final Exception e) {
      e.printStackTrace();
      throw new RuntimeException(e);
    }
    // Emit watermark to downstream operators
    emitOutputWatermark();
    checkAndFinishBundle();
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

  /**
   * Trigger timers for current key.
   * When triggering, it emits the windowed data to downstream operators.
   * @param processingTime processing time
   * @param synchronizedTime synchronized time
   */
  private int triggerTimers(final Instant processingTime,
                            final Instant synchronizedTime,
                            final Watermark triggerWatermark) {

    inMemoryTimerInternalsFactory.inputWatermarkTime = new Instant(triggerWatermark.getTimestamp());
    inMemoryTimerInternalsFactory.processingTime = processingTime;
    inMemoryTimerInternalsFactory.synchronizedProcessingTime = synchronizedTime;

    for (NemoTimerInternals timerInternals : inMemoryTimerInternalsFactory.timerInternalsMap.values()) {
      timerInternals.setCurrentInputWatermarkTime(new Instant(triggerWatermark.getTimestamp()));
      timerInternals.setCurrentProcessingTime(processingTime);
      timerInternals.setCurrentSynchronizedProcessingTime(synchronizedTime);
    }

    Pair<K, TimerInternals.TimerData> timer = inMemoryTimerInternalsFactory.getNextTimer();

    int count = 0;
    while (timer != null) {
      count += 1;
      // Trigger timers and emit windowed data
      final KeyedWorkItem<K, InputT> timerWorkItem =
        KeyedWorkItems.timersWorkItem(timer.left(), Collections.singletonList(timer.right()));

      // The DoFnRunner interface requires WindowedValue,
      // but this windowed value is actually not used in the ReduceFnRunner internal.
      getDoFnRunner().processElement(WindowedValue.valueInGlobalWindow(timerWorkItem));

      inMemoryTimerInternalsFactory.removeTimerForKeyIfEmpty(timer.left());
      // Remove states
      inMemoryStateInternalsFactory.removeNamespaceForKey(timer.left(), timer.right().getNamespace(), timer.right().getTimestamp(), getWindowingStrategy());
      timer = inMemoryTimerInternalsFactory.getNextTimer();
    }
    return count;
  }

  /**
   * Get timer data.
   */
  private List<Pair<K, TimerInternals.TimerData>> getEligibleTimers() {
    final List<Pair<K, TimerInternals.TimerData>> timerData = new LinkedList<>();

    Pair<K, TimerInternals.TimerData> timer;

    while ((timer = inMemoryTimerInternalsFactory.removeNextEventTimer()) != null) {
        timerData.add(timer);
    }
    while ((timer = inMemoryTimerInternalsFactory.removeNextProcessingTimer()) != null) {
        timerData.add(timer);
    }
    while ((timer = inMemoryTimerInternalsFactory.removeNextSynchronizedProcessingTimer()) != null) {
        timerData.add(timer);
    }
    return timerData;
  }

  @Override
  public Coder<GBKFinalState<K>> getStateCoder() {
    return new GBKFinalStateCoder<>(keyCoder, windowCoder);
  }

  @Override
  public GBKFinalState<K> getState() {
    return new GBKFinalState<>(inMemoryTimerInternalsFactory,
      inMemoryStateInternalsFactory,
      prevOutputWatermark,
      keyAndWatermarkHoldMap,
      inputWatermark);
  }

  @Override
  public void setState(GBKFinalState<K> state) {

    if (inMemoryStateInternalsFactory == null) {
      inMemoryStateInternalsFactory = state.stateInternalsFactory;
      inMemoryTimerInternalsFactory = state.timerInternalsFactory;
    } else {
      inMemoryStateInternalsFactory.setState(state.stateInternalsFactory);
      inMemoryTimerInternalsFactory.setState(state.timerInternalsFactory);
    }

    inputWatermark = state.inputWatermark;
    prevOutputWatermark = state.prevOutputWatermark;

    keyAndWatermarkHoldMap.clear();
    keyAndWatermarkHoldMap.putAll(state.keyAndWatermarkHoldMap);
  }


  /**
   * This class wraps the output collector to track the watermark hold of each key.
   */
  final class GBKWOutputCollector implements OutputCollector<WindowedValue<KV<K, OutputT>>> {
    private final OutputCollector<WindowedValue<KV<K, OutputT>>> outputCollector;
    GBKWOutputCollector(final OutputCollector<WindowedValue<KV<K, OutputT>>> outputCollector) {
      this.outputCollector = outputCollector;
    }

    @Override
    public void emit(final WindowedValue<KV<K, OutputT>> output) {

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
      // originOc.setInputTimestamp(output.getTimestamp().getMillis());
      outputCollector.emit(output);
    }

    @Override
    public void emitWatermark(final Watermark watermark) {
      outputCollector.emitWatermark(watermark);
    }

    @Override
    public <T> void emit(final String dstVertexId, final T output) {
      outputCollector.emit(dstVertexId, output);
    }
  }
}
