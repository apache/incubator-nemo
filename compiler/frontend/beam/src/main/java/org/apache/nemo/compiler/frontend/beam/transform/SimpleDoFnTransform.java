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
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.WindowingStrategy;
import org.apache.nemo.common.ir.OutputCollector;
import org.apache.nemo.common.ir.vertex.transform.Transform;
import org.apache.nemo.compiler.frontend.beam.NemoPipelineOptions;

import java.util.Collection;
import java.util.List;
import java.util.Map;

/**
 * DoFn transform implementation.
 *
 * @param <InputT> input type.
 * @param <OutputT> output type.
 */
public final class SimpleDoFnTransform<InputT, OutputT> implements
  Transform<WindowedValue<InputT>, WindowedValue<OutputT>> {

  private OutputCollector<WindowedValue<OutputT>> outputCollector;
  private final TupleTag<OutputT> mainOutputTag;
  private final List<TupleTag<?>> additionalOutputTags;
  private final Collection<PCollectionView<?>> sideInputs;
  private final WindowingStrategy<?, ?> windowingStrategy;
  private final DoFn<InputT, OutputT> doFn;
  private final SerializablePipelineOptions serializedOptions;
  private transient DoFnRunner<InputT, OutputT> doFnRunner;
  private transient SideInputReader sideInputReader;
  private transient DoFnInvoker<InputT, OutputT> doFnInvoker;
  private final Coder<InputT> inputCoder;
  private final Map<TupleTag<?>, Coder<?>> outputCoders;

  /**
   * SimpleDoFnTransform Constructor.
   *
   * @param doFn    doFn.
   * @param options Pipeline options.
   */
  public SimpleDoFnTransform(final DoFn<InputT, OutputT> doFn,
                             final Coder<InputT> inputCoder,
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
  }

  @Override
  public void prepare(final Context context, final OutputCollector<WindowedValue<OutputT>> oc) {
    // deserialize pipeline option
    final NemoPipelineOptions options = serializedOptions.get().as(NemoPipelineOptions.class);

    this.outputCollector = oc;

    // create output manager
    final DoFnRunners.OutputManager outputManager = new DefaultOutputManager<>(
      outputCollector, context, mainOutputTag);

    // create side input reader
    sideInputReader = NullSideInputReader.of(sideInputs);
    if (!sideInputs.isEmpty()) {
      sideInputReader = new BroadcastGlobalValueSideInputReader(context, sideInputs);
    }

    // create step context
    // this transform does not support state and timer.
    final StepContext stepContext = new StepContext() {
      @Override
      public StateInternals stateInternals() {
        throw new UnsupportedOperationException("Not support stateInternals in SimpleDoFnTransform");
      }

      @Override
      public TimerInternals timerInternals() {
        throw new UnsupportedOperationException("Not support timerInternals in SimpleDoFnTransform");
      }
    };

    // invoker
    doFnInvoker = DoFnInvokers.invokerFor(doFn);
    doFnInvoker.invokeSetup();

    // runner
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

    doFnRunner.startBundle();
  }

  @Override
  public void onData(final WindowedValue<InputT> data) {
    doFnRunner.processElement(data);
  }

  public DoFn getDoFn() {
    return doFn;
  }

  @Override
  public void close() {
    doFnRunner.finishBundle();
    doFnInvoker.invokeTeardown();
  }

  @Override
  public String toString() {
    final StringBuilder sb = new StringBuilder();
    sb.append("SimpleDoTransform:" + doFn);
    return sb.toString();
  }

}
