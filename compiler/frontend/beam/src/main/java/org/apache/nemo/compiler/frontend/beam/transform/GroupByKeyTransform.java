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
import org.apache.beam.runners.core.construction.SerializablePipelineOptions;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.reflect.DoFnInvoker;
import org.apache.beam.sdk.transforms.reflect.DoFnInvokers;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.WindowingStrategy;
import org.apache.nemo.common.ir.OutputCollector;
import org.apache.nemo.common.ir.vertex.transform.Transform;
import org.apache.beam.sdk.values.KV;
import org.apache.nemo.compiler.frontend.beam.NemoPipelineOptions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

/**
 * Group Beam KVs.
 * @param <K> key type.
 * @param <InputT> input type.
 */
public final class GroupByKeyTransform<K, InputT>
    implements Transform<WindowedValue<KV<K, InputT>>, WindowedValue<KV<K, Iterable<InputT>>>> {
  private static final Logger LOG = LoggerFactory.getLogger(GroupByKeyTransform.class.getName());
  private OutputCollector<WindowedValue<KV<K, Iterable<InputT>>>> outputCollector;
  private final WindowingStrategy windowingStrategy;
  private final SystemReduceFn reduceFn;
  private final TupleTag mainOutputTag;
  private final List<TupleTag<?>> additionalOutputTags;
  private final SerializablePipelineOptions serializedOptions;
  private final Collection<PCollectionView<?>> sideInputs;
  private final Map<TupleTag<?>, Coder<?>> outputCoders;
  private transient SideInputReader sideInputReader;
  private transient DoFnInvoker<KeyedWorkItem<K, InputT>, KV<K, Iterable<InputT>>> doFnInvoker;
  private transient DoFnRunner<KeyedWorkItem<K, InputT>, KV<K, Iterable<InputT>>> doFnRunner;
  private transient TimerInternalsFactory timerInternalsFactory;

  /**
   * GroupByKey constructor.
   */
  public GroupByKeyTransform(final Map<TupleTag<?>, Coder<?>> outputCoders,
                             final TupleTag<?> mainOutputTag,
                             final List<TupleTag<?>> additionalOutputTags,
                             final WindowingStrategy<?, ?> windowingStrategy,
                             final Collection<PCollectionView<?>> sideInputs,
                             final PipelineOptions options,
                             final SystemReduceFn reduceFn) {
    this.outputCoders = outputCoders;
    this.additionalOutputTags = additionalOutputTags;
    this.windowingStrategy = windowingStrategy;
    this.reduceFn = reduceFn;
    this.serializedOptions = new SerializablePipelineOptions(options);
    this.sideInputs = sideInputs;
    this.mainOutputTag = mainOutputTag;
  }

  @Override
  public void prepare(final Context context,
                      final OutputCollector<WindowedValue<KV<K, Iterable<InputT>>>> oc) {
    final NemoPipelineOptions options = serializedOptions.get().as(NemoPipelineOptions.class);
    this.outputCollector = oc;

    // create output manager
    final DoFnRunners.OutputManager outputManager = new DefaultOutputManager<>(
      outputCollector, context, mainOutputTag);

    // create side input reader
    if (!sideInputs.isEmpty()) {
      sideInputReader = new BroadcastVariableSideInputReader(context, sideInputs);
    } else {
      sideInputReader = NullSideInputReader.of(sideInputs);
    }

    timerInternalsFactory = new InMemoryTimerInternalsFactory();
    final DoFn<KeyedWorkItem<K, InputT>, KV<K, Iterable<InputT>>> doFn =
      GroupAlsoByWindowViaWindowSetNewDoFn.create(
        windowingStrategy,
        new InMemoryStateInternalsFactory(),
        timerInternalsFactory,
        sideInputReader,
        reduceFn,
        outputManager,
        mainOutputTag);

    // invoker
    doFnInvoker = DoFnInvokers.invokerFor(doFn);
    doFnInvoker.invokeSetup();

    // create step context
    // this transform does not support state and timer.
    final StepContext stepContext = new StepContext() {
      @Override
      public StateInternals stateInternals() {
        throw new UnsupportedOperationException("Not support stateInternals in DoFnTransform");
      }

      @Override
      public TimerInternals timerInternals() {
        throw new UnsupportedOperationException("Not support timerInternals in DoFnTransform");
      }
    };

    // DoFnRunners.simpleRunner takes care of all the hard stuff of running the DoFn
    // and that this approach is the standard used by most of the Beam runners
    doFnRunner = DoFnRunners.simpleRunner(
      options,
      doFn,
      sideInputReader,
      outputManager,
      mainOutputTag,
      additionalOutputTags,
      stepContext,
      null,
      outputCoders,
      windowingStrategy);

    doFnRunner.startBundle();
  }

  @Override
  public void onData(final WindowedValue<KV<K, InputT>> element) {
    // TODO #129: support window in group by key for windowed groupByKey
    final KV<K, InputT> kv = element.getValue();
    final KeyedWorkItem<K, InputT> keyedWorkItem =
      KeyedWorkItems.elementsWorkItem(kv.getKey(),
        Collections.singletonList(element.withValue(kv.getValue())));

    doFnRunner.processElement(WindowedValue.valueInGlobalWindow(keyedWorkItem));

    System.out.println("GroupByKey: " + element);
  }

  @Override
  public void close() {
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

    doFnRunner.finishBundle();
    doFnInvoker.invokeTeardown();
  }

  @Override
  public String toString() {
    final StringBuilder sb = new StringBuilder();
    sb.append("GroupByKeyTransform:");
    sb.append(super.toString());
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
    doFnRunner.processElement(
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

