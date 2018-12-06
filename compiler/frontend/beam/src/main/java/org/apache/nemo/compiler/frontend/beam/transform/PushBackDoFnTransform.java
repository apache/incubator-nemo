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

import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.display.DisplayData;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.WindowingStrategy;
import org.apache.nemo.common.ir.OutputCollector;
import org.apache.nemo.common.punctuation.Watermark;
import org.apache.nemo.compiler.frontend.beam.SideInputElement;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * DoFn transform implementation with push backs for side inputs.
 *
 * @param <InputT> input type.
 * @param <OutputT> output type.
 */
public final class PushBackDoFnTransform<InputT, OutputT> extends AbstractDoFnTransform<InputT, InputT, OutputT> {
  private static final Logger LOG = LoggerFactory.getLogger(PushBackDoFnTransform.class.getName());

  private List<WindowedValue<InputT>> curPushedBacks;
  private long curPushedBackWatermark; // Long.MAX_VALUE when no pushed-back exists.
  private long curInputWatermark;
  private long curOutputWatermark;

  /**
   * PushBackDoFnTransform Constructor.
   * @param doFn doFn
   * @param inputCoder input coder
   * @param outputCoders output coders
   * @param mainOutputTag main output tag
   * @param additionalOutputTags additional output tags
   * @param windowingStrategy windowing strategy
   * @param sideInputs side inputs
   * @param options pipeline options
   * @param displayData display data.
   */
  public PushBackDoFnTransform(final DoFn<InputT, OutputT> doFn,
                               final Coder<InputT> inputCoder,
                               final Map<TupleTag<?>, Coder<?>> outputCoders,
                               final TupleTag<OutputT> mainOutputTag,
                               final List<TupleTag<?>> additionalOutputTags,
                               final WindowingStrategy<?, ?> windowingStrategy,
                               final Map<Integer, PCollectionView<?>> sideInputs,
                               final PipelineOptions options,
                               final DisplayData displayData) {
    super(doFn, inputCoder, outputCoders, mainOutputTag,
      additionalOutputTags, windowingStrategy, sideInputs, options, displayData);
    this.curPushedBacks = new ArrayList<>();
    this.curPushedBackWatermark = Long.MAX_VALUE;
    this.curInputWatermark = Long.MIN_VALUE;
    this.curOutputWatermark = Long.MIN_VALUE;
  }

  @Override
  protected DoFn wrapDoFn(final DoFn initDoFn) {
    return initDoFn;
  }

  @Override
  public void onData(final WindowedValue data) {
    // Need to distinguish side/main inputs and push-back main inputs.
    if (data.getValue() instanceof SideInputElement) {
      // This element is a Side Input
      // TODO #287: Consider Explicit Multi-Input IR Transform
      final WindowedValue<SideInputElement> sideInputElement = (WindowedValue<SideInputElement>) data;
      final PCollectionView view = getSideInputs().get(sideInputElement.getValue().getSideInputIndex());
      getSideInputReader().addSideInputElement(view, data);

      handlePushBacks();

      // See if we can emit a new watermark, as we may have processed some pushed-back elements
      onWatermark(new Watermark(curInputWatermark));
    } else {
      // This element is the Main Input
      checkAndInvokeBundle();
      final Iterable<WindowedValue<InputT>> pushedBack =
        getPushBackRunner().processElementInReadyWindows(data);
      for (final WindowedValue wv : pushedBack) {
        curPushedBackWatermark = Math.min(curPushedBackWatermark, wv.getTimestamp().getMillis());
        curPushedBacks.add(wv);
      }
      checkAndFinishBundle();
    }
  }

  /**
   * handle pushbacks.
   */
  private void handlePushBacks() {
    // Force-finish, before (possibly) processing pushed-back data.
    //
    // Main reason:
    // {@link org.apache.beam.runners.core.SimplePushbackSideInputDoFnRunner}
    // caches for each bundle the side inputs that are not ready.
    // We need to re-start the bundle to advertise the (possibly) newly available side input.
    forceFinishBundle(); // forced

    // With the new side input added, we may be able to process some pushed-back elements.
    final List<WindowedValue<InputT>> pushedBackAgain = new ArrayList<>();
    long pushedBackAgainWatermark = Long.MAX_VALUE;
    for (final WindowedValue<InputT> curPushedBack : curPushedBacks) {
      checkAndInvokeBundle();
      final Iterable<WindowedValue<InputT>> pushedBack =
        getPushBackRunner().processElementInReadyWindows(curPushedBack);
      checkAndFinishBundle();
      for (final WindowedValue<InputT> wv : pushedBack) {
        pushedBackAgainWatermark = Math.min(pushedBackAgainWatermark, wv.getTimestamp().getMillis());
        pushedBackAgain.add(wv);
      }
    }
    curPushedBacks = pushedBackAgain;
    curPushedBackWatermark = pushedBackAgainWatermark;
  }

  @Override
  public void onWatermark(final Watermark watermark) {
    // TODO #298: Consider Processing DoFn PushBacks on Watermark
    checkAndInvokeBundle();
    curInputWatermark = watermark.getTimestamp();
    getSideInputReader().setCurrentWatermarkOfAllMainAndSideInputs(curInputWatermark);

    final long outputWatermarkCandidate = Math.min(curInputWatermark, curPushedBackWatermark);
    if (outputWatermarkCandidate > curOutputWatermark) {
      // Watermark advances!
      getOutputCollector().emitWatermark(new Watermark(outputWatermarkCandidate));
      curOutputWatermark = outputWatermarkCandidate;
    }
    checkAndFinishBundle();
  }

  @Override
  protected void beforeClose() {
    // This makes all unavailable side inputs as available empty side inputs.
    onWatermark(new Watermark(BoundedWindow.TIMESTAMP_MAX_VALUE.getMillis()));
    // All push-backs should be processed here.
    handlePushBacks();
  }

  @Override
  OutputCollector wrapOutputCollector(final OutputCollector oc) {
    return oc;
  }
}
