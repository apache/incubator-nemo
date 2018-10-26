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
import org.apache.nemo.common.Pair;
import org.apache.nemo.common.ir.vertex.transform.Watermark;
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
  private transient StateAndTimerInternalsFactory stateAndTimerInternalsFactory;

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
    this.reduceFn = reduceFn;
  }

  /**
   * This creates a new DoFn that groups elements by key and window.
   * @param doFn original doFn.
   * @return GroupAlsoByWindowViaWindowSetNewDoFn
   */
  @Override
  protected DoFn wrapDoFn(final DoFn doFn) {
    this.stateAndTimerInternalsFactory = new StateAndTimerInternalsFactory();
    // This function performs group by key and window operation
    return
      GroupAlsoByWindowViaWindowSetNewDoFn.create(
        getWindowingStrategy(),
        stateAndTimerInternalsFactory.inMemoryStateInternalsFactory,
        stateAndTimerInternalsFactory.inMemoryTimerInternalsFactory,
        getSideInputReader(),
        reduceFn,
        getOutputManager(),
        getMainOutputTag());
  }

  /**
   * It collects data for each key.
   * The collected data are emitted at {@link GroupByKeyAndWindowDoFnTransform#onWatermark(Watermark)}
   * @param element data element
   */
  @Override
  public void onData(final WindowedValue<KV<K, InputT>> element) {
    final KV<K, InputT> kv = element.getValue();
    keyToValues.putIfAbsent(kv.getKey(), new LinkedList<>());
    keyToValues.get(kv.getKey()).add(element.withValue(kv.getValue()));
  }

  /**
   * Process the collected data and trigger timers.
   * @param watermark current watermark
   * @param processingTime processing time
   * @param synchronizedTime synchronized time
   */
  private void processElementsAndTriggerTimers(final Watermark watermark,
                                               final Instant processingTime,
                                               final Instant synchronizedTime) {
    keyToValues.forEach((key, val) -> {
      // for each key
      // Process elements
      if (!val.isEmpty()) {
        final KeyedWorkItem<K, InputT> keyedWorkItem =
          KeyedWorkItems.elementsWorkItem(key, val);
        getDoFnRunner().processElement(WindowedValue.valueInGlobalWindow(keyedWorkItem));
      }

      // Trigger timers
      triggerTimers(key, watermark, processingTime, synchronizedTime);
      // Remove values
      val.clear();
    });
  }

  @Override
  public void onWatermark(Watermark watermark) {
    processElementsAndTriggerTimers(watermark, Instant.now(), Instant.now());
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
   */
  private void triggerTimers(final K key,
                             final Watermark watermark,
                             final Instant processingTime,
                             final Instant synchronizedTime) {
    final InMemoryTimerInternals timerInternals = (InMemoryTimerInternals)
      stateAndTimerInternalsFactory.inMemoryTimerInternalsFactory.timerInternalsForKey(key);
    try {
      timerInternals.advanceInputWatermark(new Instant(watermark.getTimestamp()));
      timerInternals.advanceProcessingTime(processingTime);
      timerInternals.advanceSynchronizedProcessingTime(synchronizedTime);
    } catch (final Exception e) {
      throw new RuntimeException(e);
    }

    final List<TimerInternals.TimerData> timerDataList = getTimers(timerInternals);

    if (timerDataList.isEmpty()) {
      return;
    }

    // Trigger timers and emit windowed data
    final KeyedWorkItem<K, InputT> timerWorkItem =
      KeyedWorkItems.timersWorkItem(key, timerDataList);
    getDoFnRunner().processElement(WindowedValue.valueInGlobalWindow(timerWorkItem));

    // output watermark
    // we set output watermark to the minimum of the timer data
    long outputWatermark = Long.MAX_VALUE;
    for (final TimerInternals.TimerData timer : timerDataList) {
      if (outputWatermark > timer.getTimestamp().getMillis()) {
        outputWatermark = timer.getTimestamp().getMillis();
      }
    }

    // Emit watermark to downstream operators
    timerInternals.advanceOutputWatermark(new Instant(outputWatermark));
    getOutputCollector().emitWatermark(new Watermark(outputWatermark));
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
  private List<TimerInternals.TimerData> getTimers(final InMemoryTimerInternals timerInternals) {
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
   * Maintains state internals and timer internals for keys.
   */
  final class StateAndTimerInternalsFactory {
    private final InMemoryTimerInternalsFactory inMemoryTimerInternalsFactory;
    private final InMemoryStateInternalsFactory inMemoryStateInternalsFactory;

    StateAndTimerInternalsFactory() {
      final Map<K, StateAndTimerForKey> map = new HashMap<>();
      this.inMemoryTimerInternalsFactory = new InMemoryTimerInternalsFactory(map);
      this.inMemoryStateInternalsFactory = new InMemoryStateInternalsFactory(map);
    }
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
      map.putIfAbsent(key, new StateAndTimerForKey(null, null));
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
      map.putIfAbsent(key, new StateAndTimerForKey(null, null));
      final StateAndTimerForKey stateAndTimerForKey = map.get(key);
      if (stateAndTimerForKey.timerInternals == null) {
        stateAndTimerForKey.timerInternals = new InMemoryTimerInternals();
      }
      return stateAndTimerForKey.timerInternals;
    }
  }
}

