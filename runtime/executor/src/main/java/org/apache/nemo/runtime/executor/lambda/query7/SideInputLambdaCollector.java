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
package org.apache.nemo.runtime.executor.lambda.query7;


import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.util.WindowedValue;

import org.apache.nemo.common.ir.OutputCollector;
import org.apache.nemo.common.ir.vertex.IRVertex;
import org.apache.nemo.common.punctuation.Watermark;

import org.apache.nemo.runtime.executor.lambda.SideInputProcessor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public final class SideInputLambdaCollector<O> implements OutputCollector<O> {
  private static final Logger LOG = LoggerFactory.getLogger(SideInputLambdaCollector.class.getName());

  private final IRVertex irVertex;

  int cnt = 0;


  private final SideInputProcessor sideInputProcessor;
  private final ExecutorService executorService = Executors.newCachedThreadPool();

  /**
   * Constructor of the output collector.
   * @param irVertex the ir vertex that emits the output
   */
  public SideInputLambdaCollector(
    final IRVertex irVertex,
    final SideInputProcessor<O> sideInputProcessor) {
    this.irVertex = irVertex;
    this.sideInputProcessor = sideInputProcessor;
  }

  @Override
  public void emit(final O output) {
        // CreateViewTransform (side input)
    // send to serverless
    final WindowedValue wv = ((WindowedValue) output);
    final BoundedWindow window = (BoundedWindow) wv.getWindows().iterator().next();
    LOG.info("Vertex22 Window: {} ********** {}", window, window.maxTimestamp().toString());

    final String sideInputKey = window.toString();
    final long st = System.currentTimeMillis();
    executorService.submit(() -> {
      final Object result = sideInputProcessor.processSideAndMainInput(output, sideInputKey);
      final long et = System.currentTimeMillis();
      System.out.println("!!! End time of " + sideInputKey + "::" + et + ", latency: " + (et - st));
      System.out.println("Result of " + et + ": " + result);
      return result;
    });
  }

  @Override
  public <T> void emit(final String dstVertexId, final T output) {
    // do nothing
  }

  @Override
  public void emitWatermark(final Watermark watermark) {
    // do nothing
  }
}
