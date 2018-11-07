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
package org.apache.nemo.examples.beam;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.GenerateSequence;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.nemo.compiler.frontend.beam.NemoPipelineOptions;
import org.apache.nemo.compiler.frontend.beam.NemoPipelineRunner;
import org.joda.time.Duration;
import org.joda.time.Instant;

/**
 * Computes the sum of an unbounded sequence.
 */
public final class UnboundedWindowdWordCount {
  /**
   * Private Constructor.
   */
  private UnboundedWindowdWordCount() {
  }

  /**
   * Main function.
   * @param args arguments.
   */
  public static void main(final String[] args) {
    final String outputFilePath = args[0];
    final String windowType = args[1];

    final PipelineOptions options = PipelineOptionsFactory.create().as(NemoPipelineOptions.class);
    options.setRunner(NemoPipelineRunner.class);
    options.setJobName("UnboundedWindowdWordCount");

    final Pipeline p = Pipeline.create(options);

    // Create the unbounded source.
    final PCollection<Long> unboundedSequence = p.apply(GenerateSequence
      .from(1)
      .withRate(2, Duration.standardSeconds(1))
      .withTimestampFn(num -> new Instant(num * 500))); // 10 ms interval between subsequent numbers

    // Apply windowing.
    final Window<Long> windowFn;
    windowFn = Window.<Long>into(FixedWindows.of(Duration.standardSeconds(5)));

    /*
    if (windowType.equals("fixed")) {
      windowFn = Window.<Long>into(FixedWindows.of(Duration.standardSeconds(5)));
    } else {
      windowFn = Window.<Long>into(SlidingWindows.of(Duration.standardSeconds(10))
        .every(Duration.standardSeconds(5)));
    }
    */
    final PCollection<Long> windowedSequence = unboundedSequence.apply(windowFn);

    windowedSequence
      .apply(MapElements.via(new SimpleFunction<Long, KV<Integer, Long>>() {
        @Override
        public KV<Integer, Long> apply(final Long val) {
          return KV.of((int) (val % 2), 1L);
        }
      }))
      .apply(Sum.longsPerKey())
      .apply(MapElements.via(new SimpleFunction<KV<Integer, Long>, String>() {
        @Override
        public String apply(final KV<Integer, Long> sum) {
          return String.valueOf(sum.getValue());
        }
      }))
      .apply(new WriteOneFilePerWindow(outputFilePath, 1));

    // Run the pipeline
    p.run();
  }
}
