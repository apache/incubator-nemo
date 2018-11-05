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
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.WindowingStrategy;
import org.apache.beam.sdk.values.KV;
import org.apache.nemo.common.ir.OutputCollector;
import org.apache.nemo.common.punctuation.Watermark;
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
  private Watermark currentOutputWatermark;

  private final Map<K, Watermark> keyAndWatermarkHoldMap;
  /**
   * GroupByKey constructor.
   */
  public GroupByKeyAndWindowDoFnTransform(final Map<TupleTag<?>, Coder<?>> outputCoders,
                                          final TupleTag<KV<K, Iterable<InputT>>> mainOutputTag,
                                          final List<TupleTag<?>> additionalOutputTags,
                                          final WindowingStrategy<?, ?> windowingStrategy,
                                          final Collection<PCollectionView<?>> sideInputs,
                                          final PipelineOptions options,
                                          final SystemReduceFn reduceFn) {
    super(null, /* doFn */
      null, /* inputCoder */
      outputCoders,
      mainOutputTag,
      additionalOutputTags,
      windowingStrategy,
      sideInputs,
      options);
    this.keyToValues = new HashMap<>();
    this.keyAndWatermarkHoldMap = new HashMap<>();
    this.reduceFn = reduceFn;
    this.currentOutputWatermark = new Watermark(Long.MIN_VALUE);
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
    this.inMemoryTimerInternalsFactory = new InMemoryTimerInternalsFactory(map);

    // This function performs group by key and window operation
    return
      GroupAlsoByWindowViaWindowSetNewDoFn.create(
        getWindowingStrategy(),
        inMemoryStateInternalsFactory,
        inMemoryTimerInternalsFactory,
        getSideInputReader(),
        reduceFn,
        getOutputManager(),
        getMainOutputTag());
  }

  @Override
  OutputCollector wrapOutputCollector(OutputCollector outputCollector) {
    return new GBKWOutputCollector(outputCollector);
  }

  /**
   * It collects data for each key.
   * The collected data are emitted at {@link GroupByKeyAndWindowDoFnTransform#onWatermark(Watermark)}
   * @param element data element
   */
  @Override
  public void onData(final WindowedValue<KV<K, InputT>> element) {
    // We can call Beam's DoFnRunner#processElement here,
    // but it may generate some overheads if we call the method for each data.
    // The `processElement` requires a `Iterator` of data, so we emit the buffered data every watermark.
    // TODO #250: But, this approach can delay the event processing in streaming,
    // TODO #250: if the watermark is not triggered for a long time.
    LOG.info("Receive data {}", element);
    final KV<K, InputT> kv = element.getValue();
    keyToValues.putIfAbsent(kv.getKey(), new ArrayList<>());
    keyToValues.get(kv.getKey()).add(element.withValue(kv.getValue()));
  }

  /**
   * Process the collected data and trigger timers.
   * @param inputWatermark current input watermark
   * @param processingTime processing time
   * @param synchronizedTime synchronized time
   */
  private void processElementsAndTriggerTimers(final Watermark inputWatermark,
                                               final Instant processingTime,
                                               final Instant synchronizedTime) {
    long minOutputTimestampsOfEmittedWindows = Long.MAX_VALUE;

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
        LOG.info("Before processElement for {}", inputWatermark);
        getDoFnRunner().processElement(WindowedValue.valueInGlobalWindow(keyedWorkItem));
        LOG.info("After processElement for {}", inputWatermark);
      }

      // Trigger timers
      final long minOutputTimestamp =
        triggerTimers(key, inputWatermark, processingTime, synchronizedTime);

      minOutputTimestampsOfEmittedWindows = Math.min(minOutputTimestampsOfEmittedWindows, minOutputTimestamp);

      // Remove values
      values.clear();
    }

    // Emit watermark to downstream operators

    final Watermark watermarkHold = Collections.min(keyAndWatermarkHoldMap.values());
    LOG.info("Watermark hold: {}, " +
      "inputWatermark: {}, outputWatermark: {}", watermarkHold, inputWatermark, currentOutputWatermark);
    if (watermarkHold.getTimestamp() > inputWatermark.getTimestamp()) {
      if (inputWatermark.getTimestamp() > currentOutputWatermark.getTimestamp()) {
        // progress!
        currentOutputWatermark = inputWatermark;
        LOG.info("Emit watermark: {}", currentOutputWatermark);
        getOutputCollector().emitWatermark(currentOutputWatermark);
      }
    } else {
      if (watermarkHold.getTimestamp() > currentOutputWatermark.getTimestamp()) {
        // progress!
        currentOutputWatermark = watermarkHold;
        LOG.info("Emit watermark: {}", currentOutputWatermark);
        getOutputCollector().emitWatermark(currentOutputWatermark);
      }
    }

    //LOG.info("Emit watermark: {}", inputWatermark);
    //getOutputCollector().emitWatermark(inputWatermark);
  }

  @Override
  public void onWatermark(final Watermark inputWatermark) {
    LOG.info("Receive watermark: {}", inputWatermark);
    processElementsAndTriggerTimers(inputWatermark, Instant.now(), Instant.now());
  }

  /**
   * This advances the input watermark and processing time to the timestamp max value
   * in order to emit all data.
   */
  @Override
  protected void beforeClose() {
    // Finish any pending windows by advancing the input watermark to infinity.
    processElementsAndTriggerTimers(new Watermark(BoundedWindow.TIMESTAMP_MAX_VALUE.getMillis()),
      BoundedWindow.TIMESTAMP_MAX_VALUE, BoundedWindow.TIMESTAMP_MAX_VALUE);
  }

  /**
   * Trigger times for current key.
   * When triggering, it emits the windowed data to downstream operators.
   * @param key key
   * @param watermark watermark
   * @param processingTime processing time
   * @param synchronizedTime synchronized time
   * @return the minimum output timestamp.
   * If no data is emitted, it returns Long.MAX_VALUE.
   */
  private long triggerTimers(final K key,
                             final Watermark watermark,
                             final Instant processingTime,
                             final Instant synchronizedTime) {
    final InMemoryTimerInternals timerInternals = (InMemoryTimerInternals)
      inMemoryTimerInternalsFactory.timerInternalsForKey(key);
    try {
      LOG.info("Before advance");
      timerInternals.advanceInputWatermark(new Instant(watermark.getTimestamp()));
      timerInternals.advanceProcessingTime(processingTime);
      timerInternals.advanceSynchronizedProcessingTime(synchronizedTime);
      LOG.info("After advance");
    } catch (final Exception e) {
      throw new RuntimeException(e);
    }

    final List<TimerInternals.TimerData> timerDataList = getEligibleTimers(timerInternals);
    LOG.info("After getEligibleTimers");


    if (timerDataList.isEmpty()) {
      return Long.MAX_VALUE;
    } else {

      // Trigger timers and emit windowed data
      final KeyedWorkItem<K, InputT> timerWorkItem =
        KeyedWorkItems.timersWorkItem(key, timerDataList);
      // The DoFnRunner interface requires WindowedValue,
      // but this windowed value is actually not used in the ReduceFnRunner internal.
      getDoFnRunner().processElement(WindowedValue.valueInGlobalWindow(timerWorkItem));

      // output watermark
      // we set output watermark to the minimum of the timer data
      long keyOutputTimestamp = Long.MAX_VALUE;
      for (final TimerInternals.TimerData timer : timerDataList) {
        keyOutputTimestamp = Math.min(keyOutputTimestamp, timer.getTimestamp().getMillis());
      }

      LOG.info("TimerData: {}, timerMinTimestamp: {}", timerDataList, keyOutputTimestamp);

      timerInternals.advanceOutputWatermark(new Instant(keyOutputTimestamp+1));
      keyAndWatermarkHoldMap.put(key, new Watermark(keyOutputTimestamp+1));

      return keyOutputTimestamp;
    }
  }

  @Override
  public String toString() {
    final StringBuilder sb = new StringBuilder();
    sb.append("GroupByKeyAndWindowDoFnTransform:");
    return sb.toString();
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
    private final Map<K, StateAndTimerForKey> map;

    InMemoryTimerInternalsFactory(final Map<K, StateAndTimerForKey> map) {
      this.map = map;
    }

    @Override
    public TimerInternals timerInternalsForKey(final K key) {
      map.putIfAbsent(key, new StateAndTimerForKey(null, new InMemoryTimerInternals()));
      final StateAndTimerForKey stateAndTimerForKey = map.get(key);
      if (stateAndTimerForKey.timerInternals == null) {
        stateAndTimerForKey.timerInternals = new InMemoryTimerInternals();
      }
      return stateAndTimerForKey.timerInternals;
    }
  }

  final class GBKWOutputCollector implements OutputCollector<WindowedValue<KV<K, Iterable<InputT>>>> {

    private final OutputCollector<WindowedValue<KV<K, Iterable<InputT>>>> outputCollector;
    public GBKWOutputCollector(final OutputCollector<WindowedValue<KV<K, Iterable<InputT>>>> outputCollector) {
      this.outputCollector = outputCollector;
    }

    @Override
    public void emit(WindowedValue<KV<K, Iterable<InputT>>> output) {
      keyAndWatermarkHoldMap.put(output.getValue().getKey(),
        new Watermark(output.getTimestamp().getMillis() + 1));
      outputCollector.emit(output);
    }

    @Override
    public void emitWatermark(Watermark watermark) {
      outputCollector.emitWatermark(watermark);
    }

    @Override
    public <T> void emit(String dstVertexId, T output) {
      outputCollector.emit(dstVertexId, output);
    }
  }
}
