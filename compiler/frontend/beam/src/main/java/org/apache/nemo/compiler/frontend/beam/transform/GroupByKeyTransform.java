/*
 * Copyright (C) 2018 Seoul National University
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

/**
 * Group Beam KVs.
 * @param <K> key type.
 * @param <InputT> input type.
 */
public final class GroupByKeyTransform<K, InputT>
    extends AbstractTransform<KV<K, InputT>, KeyedWorkItem<K, InputT>, KV<K, Iterable<InputT>>> {
  private static final Logger LOG = LoggerFactory.getLogger(GroupByKeyTransform.class.getName());

  private final SystemReduceFn reduceFn;
  private transient TimerInternalsFactory timerInternalsFactory;

  /**
   * GroupByKey constructor.
   */
  public GroupByKeyTransform(final Map<TupleTag<?>, Coder<?>> outputCoders,
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
    this.reduceFn = reduceFn;
  }

  /**
   * This creates a new DoFn that groups elements by key and window.
   * @param doFn original doFn.
   * @return GroupAlsoByWindowViaWindowSetNewDoFn
   */
  @Override
  protected DoFn wrapDoFn(final DoFn doFn) {
    timerInternalsFactory = new InMemoryTimerInternalsFactory();
    return
      GroupAlsoByWindowViaWindowSetNewDoFn.create(
        getWindowingStrategy(),
        new InMemoryStateInternalsFactory(),
        timerInternalsFactory,
        getSideInputReader(),
        reduceFn,
        getOutputManager(),
        getMainOutputTag());
  }

  @Override
  public void onData(final WindowedValue<KV<K, InputT>> element) {
    // The GroupAlsoByWindowViaWindowSetNewDoFn requires KeyedWorkItem,
    // so we convert the KV to KeyedWorkItem
    final KV<K, InputT> kv = element.getValue();
    final KeyedWorkItem<K, InputT> keyedWorkItem =
      KeyedWorkItems.elementsWorkItem(kv.getKey(),
        Collections.singletonList(element.withValue(kv.getValue())));

    getDoFnRunner().processElement(WindowedValue.valueInGlobalWindow(keyedWorkItem));
  }

  /**
   * This advances the input watermark and processing time to the timestamp max value
   * in order to emit all data.
   */
  @Override
  protected void beforeClose() {
    final InMemoryTimerInternalsFactory imTimerFactory =
      (InMemoryTimerInternalsFactory) timerInternalsFactory;

    imTimerFactory.internalsMap.entrySet().stream()
      .forEach(entry -> {
        final K key = entry.getKey();
        final InMemoryTimerInternals timerInternals = entry.getValue();

        try {
          // Finish any pending windows by advancing the input watermark to infinity.
          timerInternals.advanceInputWatermark(BoundedWindow.TIMESTAMP_MAX_VALUE);

          // Finally, advance the processing time to infinity to fire any timers.
          timerInternals.advanceProcessingTime(BoundedWindow.TIMESTAMP_MAX_VALUE);
          timerInternals.advanceSynchronizedProcessingTime(BoundedWindow.TIMESTAMP_MAX_VALUE);

          fireEligibleTimers(key, timerInternals);
        } catch (final Exception e) {
          e.printStackTrace();
        }
      });
  }

  @Override
  public String toString() {
    final StringBuilder sb = new StringBuilder();
    sb.append("GroupByKeyTransform:");
    return sb.toString();
  }

  private void fireEligibleTimers(final K key,
                                  final InMemoryTimerInternals timerInternals) {
    while (true) {
      TimerInternals.TimerData timer;
      boolean hasFired = false;

      while ((timer = timerInternals.removeNextEventTimer()) != null) {
        hasFired = true;
        fireTimer(key, timer);
      }
      while ((timer = timerInternals.removeNextProcessingTimer()) != null) {
        hasFired = true;
        fireTimer(key, timer);
      }
      while ((timer = timerInternals.removeNextSynchronizedProcessingTimer()) != null) {
        hasFired = true;
        fireTimer(key, timer);
      }
      if (!hasFired) {
        break;
      }
    }
  }

  private void fireTimer(final K key,
                         final TimerInternals.TimerData timer) {
    getDoFnRunner().processElement(
      WindowedValue.valueInGlobalWindow(
        KeyedWorkItems.timersWorkItem(key, Collections.singletonList(timer))));
  }

  /**
   * InMemoryStateInternalsFactory.
   */
  final class InMemoryStateInternalsFactory implements StateInternalsFactory<K> {

    private final Map<K, StateInternals> internalsMap;

    InMemoryStateInternalsFactory() {
      this.internalsMap = new HashMap<>();
    }

    @Override
    public StateInternals stateInternalsForKey(final K key) {
      final StateInternals stateInternals =
        internalsMap.getOrDefault(key, InMemoryStateInternals.forKey(key));
      internalsMap.putIfAbsent(key, stateInternals);
      return stateInternals;
    }
  }

  /**
   * InMemoryTimerInternalsFactory.
   */
  final class InMemoryTimerInternalsFactory implements TimerInternalsFactory<K> {
    private final Map<K, InMemoryTimerInternals> internalsMap;

    InMemoryTimerInternalsFactory() {
      this.internalsMap = new HashMap<>();
    }

    @Override
    public TimerInternals timerInternalsForKey(final K key) {
      final InMemoryTimerInternals timerInternals =
        internalsMap.getOrDefault(key, new InMemoryTimerInternals());
      internalsMap.putIfAbsent(key, timerInternals);
      return timerInternals;
    }
  }
}

