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
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.WindowingStrategy;
import org.apache.nemo.common.ir.OutputCollector;
import org.apache.nemo.common.ir.vertex.transform.Transform;
import org.apache.nemo.compiler.frontend.beam.NemoPipelineOptions;

import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Stateful DoFn transform implementation.
 *
 * @param <K> key type.
 * @param <InputT> input type.
 * @param <OutputT> output type.
 */
public final class StatefulDoFnTransform<K, InputT, OutputT> implements
  Transform<WindowedValue<KV<K, InputT>>, WindowedValue<OutputT>> {

  private OutputCollector<WindowedValue<OutputT>> outputCollector;
  private final TupleTag<OutputT> mainOutputTag;
  private final List<TupleTag<?>> additionalOutputTags;
  private final Collection<PCollectionView<?>> sideInputs;
  private final WindowingStrategy<?, ?> windowingStrategy;
  private final DoFn<KV<K, InputT>, OutputT> doFn;
  private final SerializablePipelineOptions serializedOptions;
  private final Coder<KV<K, InputT>> inputCoder;
  private final Map<TupleTag<?>, Coder<?>> outputCoders;

  private transient SideInputReader sideInputReader;
  private transient NemoPipelineOptions options;
  private transient DoFnRunners.OutputManager outputManager;

  private final Map<K, DoFnRunner<KV<K, InputT>, OutputT>> doFnRunnerMap;
  private final Map<K, DoFnInvoker<KV<K, InputT>, OutputT>> doFnInvokerMap;

  /**
   * Stateful DoFn Transform Constructor.
   *
   * @param doFn    doFn.
   * @param options Pipeline options.
   */
  public StatefulDoFnTransform(final DoFn<KV<K, InputT>, OutputT> doFn,
                               final Coder<KV<K, InputT>> inputCoder,
                               final Map<TupleTag<?>, Coder<?>> outputCoders,
                               final TupleTag<OutputT> mainOutputTag,
                               final List<TupleTag<?>> additionalOutputTags,
                               final WindowingStrategy<?, ?> windowingStrategy,
                               final Collection<PCollectionView<?>> sideInputs,
                               final PipelineOptions options) {
    this.doFn = doFn;
    this.inputCoder = inputCoder;
    this.outputCoders = outputCoders;
    this.mainOutputTag = mainOutputTag;
    this.additionalOutputTags = additionalOutputTags;
    this.sideInputs = sideInputs;
    this.serializedOptions = new SerializablePipelineOptions(options);
    this.windowingStrategy = windowingStrategy;
    this.doFnRunnerMap = new HashMap<>();
    this.doFnInvokerMap = new HashMap<>();
  }

  @Override
  public void prepare(final Context context, final OutputCollector<WindowedValue<OutputT>> oc) {
    // deserialize pipeline option
    this.options = serializedOptions.get().as(NemoPipelineOptions.class);

    this.outputCollector = oc;

    // create output manager
    outputManager = new DefaultOutputManager<>(
      outputCollector, context, mainOutputTag);

    // create side input reader
    sideInputReader = NullSideInputReader.of(sideInputs);
    if (!sideInputs.isEmpty()) {
      sideInputReader = new BroadcastGlobalValueSideInputReader(context, sideInputs);
    }
  }

  @Override
  public void onData(final WindowedValue<KV<K, InputT>> data) {
    final K key = data.getValue().getKey();
    DoFnRunner<KV<K, InputT>, OutputT> doFnRunner = doFnRunnerMap.get(key);

    if (doFnRunner == null) {
      // TODO #: Implement stateInternalFactory
      final StateInternals stateInternals = InMemoryStateInternals.forKey(key);
      final TimerInternals timerInternals = new InMemoryTimerInternals();
      final StepContext stepContext = new StepContext() {
        @Override
        public StateInternals stateInternals() {
          return stateInternals;
        }

        @Override
        public TimerInternals timerInternals() {
          return timerInternals;
        }
      };

      final DoFnInvoker<KV<K, InputT>, OutputT> invoker = DoFnInvokers.invokerFor(doFn);

      doFnRunner = DoFnRunners.simpleRunner(
        options,
        doFn,
        sideInputReader,
        outputManager,
        mainOutputTag,
        additionalOutputTags,
        stepContext,
        inputCoder,
        outputCoders,
        windowingStrategy);

      doFnInvokerMap.put(key, invoker);
      doFnRunnerMap.put(key, doFnRunner);

      invoker.invokeSetup();
      doFnRunner.startBundle();
    }

    doFnRunner.processElement(data);
  }

  @Override
  public void close() {
    doFnRunnerMap.values().stream().forEach((doFnRunner) -> doFnRunner.finishBundle());
    doFnInvokerMap.values().stream().forEach((invoker) -> invoker.invokeTeardown());
  }

  @Override
  public String toString() {
    final StringBuilder sb = new StringBuilder();
    sb.append("StatefulDoFnTransform:" + doFn);
    return sb.toString();
  }
}
