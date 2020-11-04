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
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.joda.time.Duration;
import org.joda.time.Instant;

/**
 * Application for EDGAR dataset.
 * Format: ip, date, time, zone, doc_cik, access number, doc_name, code, size, idx, norefer, noagent, find, crawler.
 * Count the number of requests for each company's CIK.
 */
public final class EDGARRequestsByCIK {
  /**
   * Private Constructor.
   */
  private EDGARRequestsByCIK() {
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

    final Window<KV<String, Long>> windowFn;
    if (windowType.equals("fixed")) {
      windowFn = Window.into(FixedWindows.of(Duration.standardSeconds(5)));
    } else {
      windowFn = Window.into(SlidingWindows.of(Duration.standardSeconds(10))
        .every(Duration.standardSeconds(5)));
    }

    final PipelineOptions options = NemoPipelineOptionsFactory.create();
    options.setJobName("EDGAR: Requests by CIK");

    final Pipeline p = Pipeline.create(options);

    final PCollection<KV<String, Long>> source = GenericSourceSink.read(p, inputFilePath)
      .apply(ParDo.of(new DoFn<String, KV<String, Long>>() {
        @ProcessElement
        public void processElement(@DoFn.Element final String elem,
                                   final OutputReceiver<KV<String, Long>> out) {
          final String[] splitt = elem.split(",");
          out.outputWithTimestamp(KV.of(splitt[4], 1L), Instant.parse(splitt[1] + "T" + splitt[2] + "Z"));
        }
      }));
    source.apply(windowFn)
      .apply(Sum.longsPerKey())
      .apply(MapElements.via(new SimpleFunction<KV<String, Long>, String>() {
        @Override
        public String apply(final KV<String, Long> kv) {
          return kv.getKey() + ": " + kv.getValue();
        }
      }))
      .apply(new WriteOneFilePerWindow(outputFilePath, 1));

    p.run().waitUntilFinish();
  }
}
