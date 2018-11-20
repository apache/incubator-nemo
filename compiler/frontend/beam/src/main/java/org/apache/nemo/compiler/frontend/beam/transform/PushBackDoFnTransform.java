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
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.WindowingStrategy;
import org.apache.nemo.common.ir.OutputCollector;
import org.apache.nemo.common.punctuation.Watermark;
import org.apache.nemo.common.SideInputElement;
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
   * DoFnTransform Constructor.
   *
   * @param doFn    doFn.
   * @param options Pipeline options.
   */
  public PushBackDoFnTransform(final DoFn<InputT, OutputT> doFn,
                               final Coder<InputT> inputCoder,
                               final Map<TupleTag<?>, Coder<?>> outputCoders,
                               final TupleTag<OutputT> mainOutputTag,
                               final List<TupleTag<?>> additionalOutputTags,
                               final WindowingStrategy<?, ?> windowingStrategy,
                               final Map<Integer, PCollectionView<?>> sideInputs,
                               final PipelineOptions options) {
    super(doFn, inputCoder, outputCoders, mainOutputTag,
      additionalOutputTags, windowingStrategy, sideInputs, options);
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
  public void onData(final Object data) {
    // Need to distinguish side/main inputs and push-back main inputs.
    if (data instanceof SideInputElement) {
      // This element is a Side Input
      final SideInputElement sideInputElement = (SideInputElement) data;
      // TODO #287: Consider Explicit Multi-Input IR Transform

      // Flush out any current bundle-related states in the DoFn,
      // as this sideinput may trigger the processing of pushed-back data.
      checkAndFinishBundle(true); // forced

      checkAndInvokeBundle();
      final PCollectionView view = getSideInputs().get(sideInputElement.getSideInputIndex());
      final WindowedValue sideInputData = sideInputElement.getSideInputValue();
      getSideInputHandler().addSideInputValue(view, sideInputData);

      // With the new side input added, we may be able to process some pushed-back elements.
      final List<WindowedValue<InputT>> pushedBackAgain = new ArrayList<>();
      long pushedBackAgainWatermark = Long.MAX_VALUE;
      for (WindowedValue<InputT> curPushedBack : curPushedBacks) {
        final Iterable<WindowedValue<InputT>> pushedBack =
          getPushBackRunner().processElementInReadyWindows(curPushedBack);
        for (final WindowedValue<InputT> wv : pushedBack) {
          pushedBackAgainWatermark = Math.min(pushedBackAgainWatermark, wv.getTimestamp().getMillis());
          pushedBackAgain.add(wv);
        }
      }
      curPushedBacks = pushedBackAgain;
      curPushedBackWatermark = pushedBackAgainWatermark;
      checkAndFinishBundle(false);

      // See if we can emit a new watermark, as we may have processed some pushed-back elements
      onWatermark(new Watermark(curInputWatermark));
    } else {
      // This element is the Main Input
      final WindowedValue<InputT> mainInputElement = (WindowedValue<InputT>) data;
      checkAndInvokeBundle();
      final Iterable<WindowedValue<InputT>> pushedBack =
        getPushBackRunner().processElementInReadyWindows(mainInputElement);
      for (final WindowedValue wv : pushedBack) {
        curPushedBackWatermark = Math.min(curPushedBackWatermark, wv.getTimestamp().getMillis());
        curPushedBacks.add(wv);
      }
      checkAndFinishBundle(false);
    }
  }

  @Override
  public void onWatermark(final Watermark watermark) {
    curInputWatermark = watermark.getTimestamp();
    getSideInputHandler().trackCurWatermark(curInputWatermark);

    final long minOfInputAndPushback = Math.min(curInputWatermark, curPushedBackWatermark);
    if (minOfInputAndPushback > curOutputWatermark) {
      // Watermark advances!
      getOutputCollector().emitWatermark(new Watermark(minOfInputAndPushback));
      curOutputWatermark = minOfInputAndPushback;
    }

    if (watermark.getTimestamp() >= BoundedWindow.TIMESTAMP_MAX_VALUE.getMillis()) {
      hardFlushAllPushedbacks();
    }
  }

  private void hardFlushAllPushedbacks() {
    checkAndInvokeBundle();
    // Instead of using the PushBackRunner, we directly use the DoFnRunner to not wait for sideinputs.
    curPushedBacks.forEach(wv -> getDoFnRunner().processElement(wv));
    curPushedBacks.clear();
    checkAndFinishBundle(true);
  }

  @Override
  protected void beforeClose() {
    hardFlushAllPushedbacks();
  }

  @Override
  OutputCollector wrapOutputCollector(final OutputCollector oc) {
    return oc;
  }

  @Override
  public String toString() {
    final StringBuilder sb = new StringBuilder();
    sb.append("DoTransformWithPushback:" + getDoFn());
    return sb.toString();
  }
}
