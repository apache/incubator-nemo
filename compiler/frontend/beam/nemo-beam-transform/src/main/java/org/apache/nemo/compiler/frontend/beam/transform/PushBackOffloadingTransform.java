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
import org.apache.beam.sdk.transforms.display.DisplayData;
import org.apache.beam.sdk.transforms.reflect.DoFnInvoker;
import org.apache.beam.sdk.transforms.reflect.DoFnInvokers;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.WindowingStrategy;
import org.apache.nemo.common.OffloadingTransform;
import org.apache.nemo.common.Pair;
import org.apache.nemo.common.ir.OutputCollector;
import org.apache.nemo.common.punctuation.Watermark;
import org.apache.nemo.compiler.frontend.beam.InMemorySideInputReader;
import org.apache.nemo.compiler.frontend.beam.NemoPipelineOptions;
import org.apache.nemo.compiler.frontend.beam.SideInputElement;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

/**
 * DoFn transform implementation with push backs for side inputs.
 */
public final class PushBackOffloadingTransform<InputT, OutputT> implements OffloadingTransform<Pair<WindowedValue<SideInputElement>, List<WindowedValue>>, WindowedValue<OutputT>> {
  private static final Logger LOG = LoggerFactory.getLogger(PushBackDoFnTransform.class.getName());
  private List<WindowedValue<InputT>> curPushedBacks;
  private long curPushedBackWatermark; // Long.MAX_VALUE when no pushed-back exists.
  private long curInputWatermark;
  private long curOutputWatermark;

  private final TupleTag<OutputT> mainOutputTag;
  private final List<TupleTag<?>> additionalOutputTags;
  private final Map<Integer, PCollectionView<?>> sideInputs;
  private final WindowingStrategy<?, ?> windowingStrategy;
  private final DoFn<InputT, OutputT> doFn;
  private final SerializablePipelineOptions serializedOptions;
  private final Coder<InputT> inputCoder;
  private final Map<TupleTag<?>, Coder<?>> outputCoders;


  private transient OutputCollector<WindowedValue<OutputT>> outputCollector;
  private transient DoFnRunner<InputT, OutputT> doFnRunner;


  // null when there is no side input.
  private transient PushbackSideInputDoFnRunner<InputT, OutputT> pushBackRunner;

  private transient DoFnInvoker<InputT, OutputT> doFnInvoker;
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
  private transient Context context;
  /**
   * PushBackDoFnTransform Constructor.
   */
  public PushBackOffloadingTransform(final DoFn<InputT, OutputT> doFn,
                                     final Coder<InputT> inputCoder,
                                     final Map<TupleTag<?>, Coder<?>> outputCoders,
                                     final TupleTag<OutputT> mainOutputTag,
                                     final List<TupleTag<?>> additionalOutputTags,
                                     final WindowingStrategy<?, ?> windowingStrategy,
                                     final Map<Integer, PCollectionView<?>> sideInputs,
                                     final PipelineOptions options,
                                     final DisplayData displayData) {
    this.doFn = doFn;
    this.inputCoder = inputCoder;
    this.outputCoders = outputCoders;
    this.mainOutputTag = mainOutputTag;
    this.additionalOutputTags = additionalOutputTags;
    this.sideInputs = sideInputs;
    this.serializedOptions = new SerializablePipelineOptions(options);
    this.windowingStrategy = windowingStrategy;
    this.displayData = displayData;
    this.curPushedBacks = new ArrayList<>();
    this.curPushedBackWatermark = Long.MAX_VALUE;
    this.curInputWatermark = Long.MIN_VALUE;
    this.curOutputWatermark = Long.MIN_VALUE;
  }


  /**
   * Checks whether the bundle is finished or not.
   * Starts the bundle if it is done.
   *
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
  public void prepare(Context context, OutputCollector<WindowedValue<OutputT>> oc) {
    // deserialize pipeline option
    final NemoPipelineOptions options = serializedOptions.get().as(NemoPipelineOptions.class);
    this.outputCollector = oc;
    this.context = context;

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

    // invoker
    doFnInvoker = DoFnInvokers.invokerFor(doFn);
    doFnInvoker.invokeSetup();

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
      inputCoder,
      outputCoders,
      windowingStrategy);

    pushBackRunner = sideInputs.isEmpty()
      ? null
      : SimplePushbackSideInputDoFnRunner.<InputT, OutputT>create(doFnRunner, sideInputs.values(), sideInputReader);
  }

  @Override
  public void onData(final Pair<WindowedValue<SideInputElement>, List<WindowedValue>> data) {
    final WindowedValue<SideInputElement> sideData = data.left();
    final List<WindowedValue> mainData = data.right();

    checkAndInvokeBundle();
    for (final WindowedValue main : mainData) {
      // This element is the Main Input
      final Iterable<WindowedValue<InputT>> pushedBack =
        pushBackRunner.processElementInReadyWindows(main);
      for (final WindowedValue wv : pushedBack) {
        curPushedBackWatermark = Math.min(curPushedBackWatermark, wv.getTimestamp().getMillis());
        curPushedBacks.add(wv);
      }
      //System.out.println("pushback count: " + cnt);
    }
    checkAndFinishBundle();


    // Need to distinguish side/main inputs and push-back main inputs.
    // This element is a Side Input
    final long st = System.currentTimeMillis();
    LOG.info("Receive Side input at {}: {}", this.hashCode(), sideData);
    System.out.println("Side input start time: " + System.currentTimeMillis() + " " + sideData);
    //System.out.println("Receive Side input at " + data);
    // TODO #287: Consider Explicit Multi-Input IR Transform
    final PCollectionView view = sideInputs.get(sideData.getValue().getSideInputIndex());
    sideInputReader.addSideInputElement(view, (WindowedValue) sideData);

    //LOG.info("{}, Add side input at {}: {}", System.currentTimeMillis() - st, this.hashCode(), data);

    int cnt = handlePushBacks();
    LOG.info("{}, Handle pushback cnt: {} at {}: {}", System.currentTimeMillis() - st,
      cnt, this.hashCode(), data);
    //System.out.println("{}, Handle pushback cnt: " + cnt + " data : " + data);

    // See if we can emit a new watermark, as we may have processed some pushed-back elements
    onWatermark(new Watermark(curInputWatermark));
    //LOG.info("{}, On watermark at {}: {}", System.currentTimeMillis() - st, this.hashCode(), data);
    System.out.println("Pushback latency: " + (System.currentTimeMillis() - st) +" of " + data);
  }


  private int handlePushBacks() {
    // Force-finish, before (possibly) processing pushed-back data.
    //
    // Main reason:
    // {@link org.apache.beam.runners.core.SimplePushbackSideInputDoFnRunner}
    // caches for each bundle the side inputs that are not ready.
    // We need to re-start the bundle to advertise the (possibly) newly available side input.
    forceFinishBundle(); // forced

    // With the new side input added, we may be able to process some pushed-back elements.
    checkAndInvokeBundle();
    final List<WindowedValue<InputT>> pushedBackAgain = new LinkedList<>();
    long pushedBackAgainWatermark = Long.MAX_VALUE;
    int cnt = 0;
    for (final WindowedValue<InputT> curPushedBack : curPushedBacks) {
      final Iterable<WindowedValue<InputT>> pushedBack =
        pushBackRunner.processElementInReadyWindows(curPushedBack);
      cnt += 1;
      for (final WindowedValue<InputT> wv : pushedBack) {
        pushedBackAgainWatermark = Math.min(pushedBackAgainWatermark, wv.getTimestamp().getMillis());
        pushedBackAgain.add(wv);
      }
    }

    checkAndFinishBundle();

    LOG.info("Pushback again size: {}", pushedBackAgain.size());

    curPushedBacks = pushedBackAgain;
    curPushedBackWatermark = pushedBackAgainWatermark;
    return cnt;
  }

  @Override
  public void onWatermark(final Watermark watermark) {
    checkAndInvokeBundle();
    curInputWatermark = watermark.getTimestamp();
    sideInputReader.setCurrentWatermarkOfAllMainAndSideInputs(curInputWatermark);

    final long outputWatermarkCandidate = Math.min(curInputWatermark, curPushedBackWatermark);
    if (outputWatermarkCandidate > curOutputWatermark) {
      // Watermark advances!
      outputCollector.emitWatermark(new Watermark(outputWatermarkCandidate));
      curOutputWatermark = outputWatermarkCandidate;
    }
    checkAndFinishBundle();
  }

  @Override
  public void close() {
    // This makes all unavailable side inputs as available empty side inputs.
    onWatermark(new Watermark(BoundedWindow.TIMESTAMP_MAX_VALUE.getMillis()));
    // All push-backs should be processed here.
    handlePushBacks();

    forceFinishBundle();
    doFnInvoker.invokeTeardown();
  }
}
