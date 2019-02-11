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
import org.apache.nemo.common.OffloadingTransform;
import org.apache.nemo.common.Pair;
import org.apache.nemo.common.ir.OutputCollector;
import org.apache.nemo.common.punctuation.Watermark;
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
public final class PushBackOffloadingTransform<InputT, OutputT> implements OffloadingTransform<Pair<WindowedValue<SideInputElement>, List<WindowedValue>>, WindowedValue> {
  private static final Logger LOG = LoggerFactory.getLogger(PushBackDoFnTransform.class.getName());
  private final PushBackDoFnTransform pushBackDoFnTransform;

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
                                     final DisplayData displayData,
                                     final Coder mainCoder,
                                     final Coder sideCoder) {
    this.pushBackDoFnTransform = new PushBackDoFnTransform(doFn, inputCoder, outputCoders, mainOutputTag,
      additionalOutputTags, windowingStrategy, sideInputs, options, displayData, mainCoder, sideCoder);
    this.pushBackDoFnTransform.setOffloading(false);
  }

  @Override
  public void prepare(Context context, OutputCollector<WindowedValue> outputCollector) {
    pushBackDoFnTransform.prepare(context, outputCollector);
  }

  @Override
  public void onData(final Pair<WindowedValue<SideInputElement>, List<WindowedValue>> data) {
    final WindowedValue<SideInputElement> side = data.left();
    final List<WindowedValue> main = data.right();

    pushBackDoFnTransform.onData(side);
    for (final WindowedValue d : main) {
      pushBackDoFnTransform.onData(d);
    }
  }

  @Override
  public void onWatermark(final Watermark watermark) {
    pushBackDoFnTransform.onWatermark(watermark);
  }

  @Override
  public void close() {
    pushBackDoFnTransform.close();
  }
}
