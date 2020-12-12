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
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.SlidingWindows;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.PCollection;
import org.joda.time.Duration;
import org.joda.time.Instant;

/**
 * Application for EDGAR dataset.
 * Format: ip, date, time, zone, doc_cik, access number, doc_name, code, size, idx, norefer, noagent, find, crawler.
 * Calculate the average document size of the requests.
 */
public final class EDGARAvgDocSize {
  /**
   * Private Constructor.
   */
  private EDGARAvgDocSize() {
  }

  /**
   * Main function for the BEAM program.
   *
   * @param args arguments.
   */
  public static void main(final String[] args) {
    final String inputFilePath = args[0];
    final String windowType = args[1];
    final String outputFilePath = args[2];

    final Window<Number> windowFn;
    if (windowType.equals("fixed")) {
      windowFn = Window.into(FixedWindows.of(Duration.standardSeconds(5)));
    } else {
      windowFn = Window.into(SlidingWindows.of(Duration.standardSeconds(10))
        .every(Duration.standardSeconds(5)));
    }

    final PipelineOptions options = NemoPipelineOptionsFactory.create();
    options.setJobName("EDGAR: Average document size per window");

    final Pipeline p = Pipeline.create(options);

    final PCollection<Number> source = GenericSourceSink.read(p, inputFilePath)
      .apply(ParDo.of(new DoFn<String, Number>() {
        @ProcessElement
        public void processElement(@DoFn.Element final String elem,
                                   final OutputReceiver<Number> out) {
          final String[] splitt = elem.split(",");
          out.outputWithTimestamp(Double.valueOf(splitt[8]).longValue(),
            Instant.parse(splitt[1] + "T" + splitt[2] + "Z"));
        }
      }));
    source.apply(windowFn)
      .apply(Mean.globally().withoutDefaults())
      .apply(MapElements.via(new SimpleFunction<Double, String>() {
        @Override
        public String apply(final Double d) {
          return d.toString();
        }
      }))
      .apply(new WriteOneFilePerWindow(outputFilePath, 1));

    p.run().waitUntilFinish();
  }
}
