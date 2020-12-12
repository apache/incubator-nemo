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
import org.apache.beam.runners.core.construction.SerializablePipelineOptions;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.DoFnSchemaInformation;
import org.apache.beam.sdk.transforms.display.DisplayData;
import org.apache.beam.sdk.transforms.reflect.DoFnInvoker;
import org.apache.beam.sdk.transforms.reflect.DoFnInvokers;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.WindowingStrategy;
import org.apache.nemo.common.ir.OutputCollector;
import org.apache.nemo.common.ir.vertex.transform.Transform;
import org.apache.nemo.compiler.frontend.beam.InMemorySideInputReader;
import org.apache.nemo.compiler.frontend.beam.NemoPipelineOptions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * This is a base class for Beam DoFn Transforms.
 *
 * @param <InputT>  input type.
 * @param <InterT>  intermediate type.
 * @param <OutputT> output type.
 */
public abstract class AbstractDoFnTransform<InputT, InterT, OutputT> implements
  Transform<WindowedValue<InputT>, WindowedValue<OutputT>> {
  private static final Logger LOG = LoggerFactory.getLogger(AbstractDoFnTransform.class.getName());

  private final TupleTag<OutputT> mainOutputTag;
  private final List<TupleTag<?>> additionalOutputTags;
  private final Map<Integer, PCollectionView<?>> sideInputs;
  private final WindowingStrategy<?, ?> windowingStrategy;
  private final DoFn<InterT, OutputT> doFn;
  private final SerializablePipelineOptions serializedOptions;
  private final Coder<InputT> inputCoder;
  private final Map<TupleTag<?>, Coder<?>> outputCoders;

  private transient OutputCollector<WindowedValue<OutputT>> outputCollector;
  private transient DoFnRunner<InterT, OutputT> doFnRunner;

  // null when there is no side input.
  private transient PushbackSideInputDoFnRunner<InterT, OutputT> pushBackRunner;

  private transient DoFnInvoker<InterT, OutputT> doFnInvoker;
  private transient DoFnRunners.OutputManager outputManager;
  private transient InMemorySideInputReader sideInputReader;

  // Variables for bundle.
  // We consider count and time millis for start/finish bundle.
  // If # of processed elements > bundleSize
  // or elapsed time > bundleMillis, we finish the current bundle and start a new one
  private transient long bundleSize;
  private transient long bundleMillis;
  private long prevBundleStartTime;
  private long currBundleCount = 0;
  private boolean bundleFinished = true;
  private final DisplayData displayData;

  private final DoFnSchemaInformation doFnSchemaInformation;

  private final Map<String, PCollectionView<?>> sideInputMapping;

  /**
   * AbstractDoFnTransform constructor.
   *
   * @param doFn                 doFn
   * @param inputCoder           input coder
   * @param outputCoders         output coders
   * @param mainOutputTag        main output tag
   * @param additionalOutputTags additional output tags
   * @param windowingStrategy    windowing strategy
   * @param sideInputs           side inputs
   * @param options              pipeline options
   * @param displayData          display data.
   * @param doFnSchemaInformation doFn schema information.
   * @param sideInputMapping      side input mapping.
   */
  public AbstractDoFnTransform(final DoFn<InterT, OutputT> doFn,
                               final Coder<InputT> inputCoder,
                               final Map<TupleTag<?>, Coder<?>> outputCoders,
                               final TupleTag<OutputT> mainOutputTag,
                               final List<TupleTag<?>> additionalOutputTags,
                               final WindowingStrategy<?, ?> windowingStrategy,
                               final Map<Integer, PCollectionView<?>> sideInputs,
                               final PipelineOptions options,
                               final DisplayData displayData,
                               final DoFnSchemaInformation doFnSchemaInformation,
                               final Map<String, PCollectionView<?>> sideInputMapping) {
    this.doFn = doFn;
    this.inputCoder = inputCoder;
    this.outputCoders = outputCoders;
    this.mainOutputTag = mainOutputTag;
    this.additionalOutputTags = additionalOutputTags;
    this.sideInputs = sideInputs;
    this.serializedOptions = new SerializablePipelineOptions(options);
    this.windowingStrategy = windowingStrategy;
    this.displayData = displayData;
    this.doFnSchemaInformation = doFnSchemaInformation;
    this.sideInputMapping = sideInputMapping;
  }

  /**
   * Getter function for side inputs.
   *
   * @return the side inputs
   */
  final Map<Integer, PCollectionView<?>> getSideInputs() {
    return sideInputs;
  }

  /**
   * Getter function for output manager.
   *
   * @return the output manager.
   */
  final DoFnRunners.OutputManager getOutputManager() {
    return outputManager;
  }

  /**
   * Getter function for windowing strategy.
   *
   * @return the windowing strategy.
   */
  final WindowingStrategy getWindowingStrategy() {
    return windowingStrategy;
  }

  /**
   * Getter function for output tag.
   *
   * @return main output tag.
   */
  final TupleTag<OutputT> getMainOutputTag() {
    return mainOutputTag;
  }

  /**
   * Getter function for DoFn runner.
   *
   * @return DoFn runner.
   */
  final DoFnRunner<InterT, OutputT> getDoFnRunner() {
    return doFnRunner;
  }

  /**
   * Getter function for push back runner.
   *
   * @return push back runner.
   */
  final PushbackSideInputDoFnRunner<InterT, OutputT> getPushBackRunner() {
    return pushBackRunner;
  }

  /**
   * Getter function for side input reader.
   *
   * @return side input reader.
   */
  final InMemorySideInputReader getSideInputReader() {
    return sideInputReader;
  }

  /**
   * Getter function for DoFn.
   *
   * @return DoFn.
   */
  public final DoFn getDoFn() {
    return doFn;
  }

  /**
   * Checks whether the bundle is finished or not.
   * Starts the bundle if it is done.
   * <p>
   * TODO #263: Partial Combining for Beam Streaming
   * We may want to use separate methods for doFnRunner/pushBackRunner
   * (same applies to the other bundle-related methods)
   */
  final void checkAndInvokeBundle() {
    if (bundleFinished) {
      bundleFinished = false;
      if (pushBackRunner == null) {
        doFnRunner.startBundle();
      } else {
        pushBackRunner.startBundle();
      }
      prevBundleStartTime = System.currentTimeMillis();
      currBundleCount = 0;
    }
    currBundleCount += 1;
  }

  /**
   * Checks whether it is time to finish the bundle and finish it.
   */
  final void checkAndFinishBundle() {
    if (!bundleFinished) {
      if (currBundleCount >= bundleSize || System.currentTimeMillis() - prevBundleStartTime >= bundleMillis) {
        bundleFinished = true;
        if (pushBackRunner == null) {
          doFnRunner.finishBundle();
        } else {
          pushBackRunner.finishBundle();
        }
      }
    }
  }

  /**
   * Finish bundle without checking for conditions.
   */
  final void forceFinishBundle() {
    if (!bundleFinished) {
      bundleFinished = true;
      if (pushBackRunner == null) {
        doFnRunner.finishBundle();
      } else {
        pushBackRunner.finishBundle();
      }
    }
  }

  @Override
  public final void prepare(final Context context, final OutputCollector<WindowedValue<OutputT>> oc) {
    // deserialize pipeline option
    final NemoPipelineOptions options = serializedOptions.get().as(NemoPipelineOptions.class);
    this.outputCollector = wrapOutputCollector(oc);

    this.bundleMillis = options.getMaxBundleTimeMills();
    this.bundleSize = options.getMaxBundleSize();

    // create output manager
    outputManager = new DefaultOutputManager<>(outputCollector, mainOutputTag);

    // create side input reader
    sideInputReader = new InMemorySideInputReader(new ArrayList<>(sideInputs.values()));

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

    final DoFn wrappedDoFn = wrapDoFn(doFn);

    // invoker
    doFnInvoker = DoFnInvokers.invokerFor(wrappedDoFn);
    doFnInvoker.invokeSetup();

    // DoFnRunners.simpleRunner takes care of all the hard stuff of running the DoFn
    // and that this approach is the standard used by most of the Beam runners
    doFnRunner = DoFnRunners.simpleRunner(
      options,
      wrappedDoFn,
      sideInputReader,
      outputManager,
      mainOutputTag,
      additionalOutputTags,
      stepContext,
      inputCoder,
      outputCoders,
      windowingStrategy,
      doFnSchemaInformation,
      sideInputMapping);

    pushBackRunner = sideInputs.isEmpty()
      ? null
      : SimplePushbackSideInputDoFnRunner.<InterT, OutputT>create(doFnRunner, sideInputs.values(), sideInputReader);
  }

  /**
   * Getter function for output collector.
   *
   * @return output collector.
   */
  public final OutputCollector<WindowedValue<OutputT>> getOutputCollector() {
    return outputCollector;
  }

  @Override
  public final void close() {
    beforeClose();
    forceFinishBundle();
    doFnInvoker.invokeTeardown();
  }

  @Override
  public final String toString() {
    return this.getClass().getSimpleName() + " / " + displayData.toString().replace(":", " / ");
  }

  /**
   * An abstract function that wraps the original doFn.
   *
   * @param originalDoFn the original doFn.
   * @return wrapped doFn.
   */
  abstract DoFn wrapDoFn(DoFn originalDoFn);

  /**
   * An abstract function that wraps the original output collector.
   *
   * @param oc the original outputCollector.
   * @return wrapped output collector.
   */
  abstract OutputCollector wrapOutputCollector(OutputCollector oc);

  /**
   * An abstract function that is called before close.
   */
  abstract void beforeClose();
}
