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
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.display.DisplayData;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.PaneInfo;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.WindowingStrategy;
import org.apache.beam.sdk.values.KV;
import org.apache.nemo.common.ir.OutputCollector;
import org.apache.nemo.common.punctuation.Watermark;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

/**
 * Groups elements according to key and window.
 * @param <K> key type.
 * @param <InputT> input type.
 */
public final class GroupByKeyAndWindowDoFnTransform<K, InputT>
  extends AbstractDoFnTransform<KV<K, InputT>, KeyedWorkItem<K, InputT>, KV<K, Iterable<InputT>>> {
  private static final Logger LOG = LoggerFactory.getLogger(GroupByKeyAndWindowDoFnTransform.class.getName());

  private final SystemReduceFn reduceFn;
  private final Map<K, List<WindowedValue<InputT>>> keyToValues;
  private transient InMemoryTimerInternalsFactory inMemoryTimerInternalsFactory;
  private transient InMemoryStateInternalsFactory inMemoryStateInternalsFactory;
  private Watermark prevOutputWatermark;
  private final Map<K, Watermark> keyAndWatermarkHoldMap;
  private final WindowingStrategy windowingStrategy;
  private Watermark inputWatermark;

  /**
   * GroupByKey constructor.
   */
  public GroupByKeyAndWindowDoFnTransform(final Map<TupleTag<?>, Coder<?>> outputCoders,
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
    this.keyToValues = new HashMap<>();
    this.reduceFn = reduceFn;
    this.prevOutputWatermark = new Watermark(Long.MIN_VALUE);
    this.inputWatermark = new Watermark(Long.MIN_VALUE);
    this.keyAndWatermarkHoldMap = new HashMap<>();
    this.windowingStrategy = windowingStrategy;
  }

  /**
   * This creates a new DoFn that groups elements by key and window.
   * @param doFn original doFn.
   * @return GroupAlsoByWindowViaWindowSetNewDoFn
   */
  @Override
  protected DoFn wrapDoFn(final DoFn doFn) {
    final Map<K, StateAndTimerForKey> map = new HashMap<>();
    this.inMemoryStateInternalsFactory = new InMemoryStateInternalsFactory(map);
    this.inMemoryTimerInternalsFactory = new InMemoryTimerInternalsFactory();

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
    return new GBKWOutputCollector(oc);
  }

  /**
   * It collects data for each key.
   * The collected data are emitted at {@link GroupByKeyAndWindowDoFnTransform#onWatermark(Watermark)}
   * @param element data element
   */
  @Override
  public void onData(final WindowedValue<KV<K, InputT>> element) {
    // drop late data
    if (element.getTimestamp().isAfter(inputWatermark.getTimestamp())) {
      checkAndInvokeBundle();
      // We can call Beam's DoFnRunner#processElement here,
      // but it may generate some overheads if we call the method for each data.
      // The `processElement` requires a `Iterator` of data, so we emit the buffered data every watermark.
      // TODO #250: But, this approach can delay the event processing in streaming,
      // TODO #250: if the watermark is not triggered for a long time.

      final KV<K, InputT> kv = element.getValue();
      keyToValues.putIfAbsent(kv.getKey(), new ArrayList<>());
      keyToValues.get(kv.getKey()).add(element.withValue(kv.getValue()));

      checkAndFinishBundle();
    }
  }

  /**
   * Process the collected data and trigger timers.
   * @param processingTime processing time
   * @param synchronizedTime synchronized time
   */
  private void processElementsAndTriggerTimers(final Instant processingTime,
                                               final Instant synchronizedTime) {
    final long st = System.currentTimeMillis();
    for (final Map.Entry<K, List<WindowedValue<InputT>>> entry : keyToValues.entrySet()) {
      final K key = entry.getKey();
      final List<WindowedValue<InputT>> values = entry.getValue();

      // for each key
      // Process elements
      if (!values.isEmpty()) {
        final KeyedWorkItem<K, InputT> keyedWorkItem =
          KeyedWorkItems.elementsWorkItem(key, values);
        // The DoFnRunner interface requires WindowedValue,
        // but this windowed value is actually not used in the ReduceFnRunner internal.
        getDoFnRunner().processElement(WindowedValue.valueInGlobalWindow(keyedWorkItem));
        // Remove values
        values.clear();
      }
    }

    LOG.info("{} time to process element: {}", getContext().getIRVertex().getId(),
      (System.currentTimeMillis() - st));

    // Trigger timers
    triggerTimers(processingTime, synchronizedTime);

    LOG.info("{} time to trigger: {}", getContext().getIRVertex().getId(),
      (System.currentTimeMillis() - st));
  }

  /**
   * Output watermark
   * = max(prev output watermark,
   *          min(input watermark, watermark holds)).
   */
  private void emitOutputWatermark() {
    // Find min watermark hold
    final Watermark minWatermarkHold = keyAndWatermarkHoldMap.isEmpty()
      ? new Watermark(Long.MAX_VALUE) // set this to MAX, in order to just use the input watermark.
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
    checkAndInvokeBundle();
    inputWatermark = watermark;

    final long st = System.currentTimeMillis();
    processElementsAndTriggerTimers(Instant.now(), Instant.now());
    // Emit watermark to downstream operators
    emitOutputWatermark();
    checkAndFinishBundle();

    final long et = System.currentTimeMillis();
    LOG.info("{}/{} latency {}",
      getContext().getIRVertex().getId(), Thread.currentThread().getId(), (et-st));
  }

  /**
   * This advances the input watermark and processing time to the timestamp max value
   * in order to emit all data.
   */
  @Override
  protected void beforeClose() {
    // Finish any pending windows by advancing the input watermark to infinity.
    inputWatermark = new Watermark(BoundedWindow.TIMESTAMP_MAX_VALUE.getMillis());
    processElementsAndTriggerTimers(BoundedWindow.TIMESTAMP_MAX_VALUE, BoundedWindow.TIMESTAMP_MAX_VALUE);
  }

  /**
   * Trigger times for current key.
   * When triggering, it emits the windowed data to downstream operators.
   * @param processingTime processing time
   * @param synchronizedTime synchronized time
   */
  private void triggerTimers(final Instant processingTime,
                             final Instant synchronizedTime) {
    final InMemoryTimerInternals timerInternals = (InMemoryTimerInternals)
      inMemoryTimerInternalsFactory.timerInternalsForKey(null);
    try {
      timerInternals.advanceInputWatermark(new Instant(inputWatermark.getTimestamp()));
      timerInternals.advanceProcessingTime(processingTime);
      timerInternals.advanceSynchronizedProcessingTime(synchronizedTime);
    } catch (final Exception e) {
      throw new RuntimeException(e);
    }

    final List<TimerInternals.TimerData> timerDataList = getEligibleTimers(timerInternals);

    if (!timerDataList.isEmpty()) {
      for (final Map.Entry<K, List<WindowedValue<InputT>>> entry : keyToValues.entrySet()) {
        // Trigger timers and emit windowed data
        final KeyedWorkItem<K, InputT> timerWorkItem =
          KeyedWorkItems.timersWorkItem(entry.getKey(), timerDataList);
        // The DoFnRunner interface requires WindowedValue,
        // but this windowed value is actually not used in the ReduceFnRunner internal.
        getDoFnRunner().processElement(WindowedValue.valueInGlobalWindow(timerWorkItem));
      }
    }
  }

  /**
   * Get timer data.
   */
  private List<TimerInternals.TimerData> getEligibleTimers(final InMemoryTimerInternals timerInternals) {
    final List<TimerInternals.TimerData> timerData = new LinkedList<>();

    while (true) {
      TimerInternals.TimerData timer;
      boolean hasFired = false;

      while ((timer = timerInternals.removeNextEventTimer()) != null) {
        hasFired = true;
        timerData.add(timer);
      }
      while ((timer = timerInternals.removeNextProcessingTimer()) != null) {
        hasFired = true;
        timerData.add(timer);
      }
      while ((timer = timerInternals.removeNextSynchronizedProcessingTimer()) != null) {
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
   * State and timer internal.
   */
  final class StateAndTimerForKey {
    private StateInternals stateInternals;
    private TimerInternals timerInternals;

    StateAndTimerForKey(final StateInternals stateInternals,
                        final TimerInternals timerInternals) {
      this.stateInternals = stateInternals;
      this.timerInternals = timerInternals;
    }
  }

  /**
   * InMemoryStateInternalsFactory.
   */
  final class InMemoryStateInternalsFactory implements StateInternalsFactory<K> {
    private final Map<K, StateAndTimerForKey> map;

    InMemoryStateInternalsFactory(final Map<K, StateAndTimerForKey> map) {
      this.map = map;
    }

    @Override
    public StateInternals stateInternalsForKey(final K key) {
      map.putIfAbsent(key, new StateAndTimerForKey(InMemoryStateInternals.forKey(key), null));
      final StateAndTimerForKey stateAndTimerForKey = map.get(key);
      if (stateAndTimerForKey.stateInternals == null) {
        stateAndTimerForKey.stateInternals = InMemoryStateInternals.forKey(key);
      }
      return stateAndTimerForKey.stateInternals;
    }
  }

  /**
   * InMemoryTimerInternalsFactory.
   */
  final class InMemoryTimerInternalsFactory implements TimerInternalsFactory<K> {
    private final TimerInternals timerInternals;

    InMemoryTimerInternalsFactory() {
      this.timerInternals = new InMemoryTimerInternals();
    }

    @Override
    public TimerInternals timerInternalsForKey(final K key) {
      return timerInternals;
    }
  }

  /**
   * This class wraps the output collector to track the watermark hold of each key.
   */
  final class GBKWOutputCollector implements OutputCollector<WindowedValue<KV<K, Iterable<InputT>>>> {
    private final OutputCollector<WindowedValue<KV<K, Iterable<InputT>>>> outputCollector;
    GBKWOutputCollector(final OutputCollector<WindowedValue<KV<K, Iterable<InputT>>>> outputCollector) {
      this.outputCollector = outputCollector;
    }

    @Override
    public void emit(final WindowedValue<KV<K, Iterable<InputT>>> output) {

      // The watermark advances only in ON_TIME
      if (output.getPane().getTiming().equals(PaneInfo.Timing.ON_TIME)) {
        final K key = output.getValue().getKey();
        final InMemoryTimerInternals timerInternals = (InMemoryTimerInternals)
          inMemoryTimerInternalsFactory.timerInternalsForKey(key);
        keyAndWatermarkHoldMap.put(key,
          // adds the output timestamp to the watermark hold of each key
          // +1 to the output timestamp because if the window is [0-5000), the timestamp is 4999
          new Watermark(output.getTimestamp().getMillis() + 1));
        timerInternals.advanceOutputWatermark(new Instant(output.getTimestamp().getMillis() + 1));
      }
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
