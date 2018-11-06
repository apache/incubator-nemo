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
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.WindowingStrategy;
import org.apache.nemo.common.ir.OutputCollector;
import org.apache.nemo.common.punctuation.Watermark;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * DoFn transform implementation.
 *
 * @param <InputT> input type.
 * @param <OutputT> output type.
 */
public final class DoFnTransform<InputT, OutputT> extends AbstractDoFnTransform<InputT, InputT, OutputT> {
  private static final Logger LOG = LoggerFactory.getLogger(DoFnTransform.class.getName());

  private transient ScheduledExecutorService bundleScheduler;
  private boolean isBundleFinished = false;

  /**
   * DoFnTransform Constructor.
   *
   * @param doFn    doFn.
   * @param options Pipeline options.
   */
  public DoFnTransform(final DoFn<InputT, OutputT> doFn,
                       final Coder<InputT> inputCoder,
                       final Map<TupleTag<?>, Coder<?>> outputCoders,
                       final TupleTag<OutputT> mainOutputTag,
                       final List<TupleTag<?>> additionalOutputTags,
                       final WindowingStrategy<?, ?> windowingStrategy,
                       final Collection<PCollectionView<?>> sideInputs,
                       final PipelineOptions options) {
    super(doFn, inputCoder, outputCoders, mainOutputTag,
      additionalOutputTags, windowingStrategy, sideInputs, options);
  }

  @Override
  protected DoFn wrapDoFn(final DoFn initDoFn) {
    bundleScheduler = Executors.newSingleThreadScheduledExecutor();
    bundleScheduler.scheduleAtFixedRate(() -> {
      isBundleFinished = true;
    }, 1, 1, TimeUnit.SECONDS);
    return initDoFn;
  }

  @Override
  public void onData(final WindowedValue<InputT> data) {
    getDoFnRunner().processElement(data);

    if (isBundleFinished) {
      LOG.info("BundleFinished: true");
      getDoFnRunner().finishBundle();
      isBundleFinished = false;
      getDoFnRunner().startBundle();
    }
  }

  @Override
  public void onWatermark(final Watermark watermark) {
    // TODO #216: We should consider push-back data that waits for side input
    // TODO #216: If there are push-back data, input watermark >= output watermark
    getOutputCollector().emitWatermark(watermark);

    if (isBundleFinished) {
      LOG.info("BundleFinished: true");
      getDoFnRunner().finishBundle();
      isBundleFinished = false;
      getDoFnRunner().startBundle();
    }
  }

  @Override
  protected void beforeClose() {
    // nothing
    bundleScheduler.shutdown();
  }

  @Override
  OutputCollector wrapOutputCollector(final OutputCollector oc) {
    return oc;
  }

  @Override
  public String toString() {
    final StringBuilder sb = new StringBuilder();
    sb.append("DoTransform:" + getDoFn() + "-" + getDoFn().getClass().toString());
    return sb.toString();
  }
}
