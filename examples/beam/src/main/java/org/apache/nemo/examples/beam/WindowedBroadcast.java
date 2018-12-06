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
import org.apache.beam.sdk.transforms.windowing.SlidingWindows;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.nemo.compiler.frontend.beam.NemoPipelineOptions;
import org.apache.nemo.compiler.frontend.beam.NemoRunner;
import org.joda.time.Duration;
import org.joda.time.Instant;

import java.util.List;

/**
 * A Windowed WordCount application.
 */
public final class WindowedBroadcast {
  /**
   * Private Constructor.
   */
  private WindowedBroadcast() {
  }

  /**
   * @param p pipeline.
   * @return source.
   */
  private static PCollection<Long> getSource(final Pipeline p) {
    return p.apply(GenerateSequence
      .from(1)
      .withRate(2, Duration.standardSeconds(1))
      .withTimestampFn(num -> new Instant(num * 500))); // 0.5 second between subsequent elements
  }
  /**
   * Main function for the MR BEAM program.
   * @param args arguments.
   */
  public static void main(final String[] args) {
    final String outputFilePath = args[0];

    final Window<Long> windowFn = Window
      .<Long>into(SlidingWindows.of(Duration.standardSeconds(2))
      .every(Duration.standardSeconds(1)));

    final PipelineOptions options = PipelineOptionsFactory.create().as(NemoPipelineOptions.class);
    options.setRunner(NemoRunner.class);
    options.setJobName("WindowedBroadcast");

    final Pipeline p = Pipeline.create(options);

    final PCollection<Long> windowedElements = getSource(p).apply(windowFn);
    final PCollectionView<List<Long>> windowedView = windowedElements.apply(View.asList());

    windowedElements.apply(ParDo.of(new DoFn<Long, String>() {
        @ProcessElement
        public void processElement(final ProcessContext c) {
          final Long anElementInTheWindow = c.element();
          final List<Long> allElementsInTheWindow = c.sideInput(windowedView);
          System.out.println(anElementInTheWindow + " / " + allElementsInTheWindow);
          if (!allElementsInTheWindow.contains(anElementInTheWindow)) {
            throw new RuntimeException(anElementInTheWindow + " not in " + allElementsInTheWindow.toString());
          } else {
            c.output(anElementInTheWindow + " is in " + allElementsInTheWindow);
          }
        }
      }).withSideInputs(windowedView)
    ).apply(new WriteOneFilePerWindow(outputFilePath, 1));

    p.run();
  }
}
