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
 * Calculate the success rate of each document.
 */
public final class EDGARDocumentSuccessRate {
  /**
   * Private Constructor.
   */
  private EDGARDocumentSuccessRate() {
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

    final Window<KV<String, Integer>> windowFn;
    if (windowType.equals("fixed")) {
      windowFn = Window.into(FixedWindows.of(Duration.standardSeconds(5)));
    } else {
      windowFn = Window.into(SlidingWindows.of(Duration.standardSeconds(10))
        .every(Duration.standardSeconds(5)));
    }

    final PipelineOptions options = NemoPipelineOptionsFactory.create();
    options.setJobName("EDGAR: Document retrieval success rate");

    final Pipeline p = Pipeline.create(options);

    final PCollection<KV<String, Integer>> source = GenericSourceSink.read(p, inputFilePath)
      .apply(ParDo.of(new DoFn<String, KV<String, Integer>>() {
        @ProcessElement
        public void processElement(@DoFn.Element final String elem,
                                   final OutputReceiver<KV<String, Integer>> out) {
          final String[] splitt = elem.split(",");
          final Integer success = splitt[7].startsWith("2") ? 1 : 0;
          out.outputWithTimestamp(KV.of(splitt[6], success), Instant.parse(splitt[1] + "T" + splitt[2] + "Z"));
        }
      }));
    source.apply(windowFn)
      .apply(Mean.perKey())
      .apply(MapElements.via(new SimpleFunction<KV<String, Double>, String>() {
        @Override
        public String apply(final KV<String, Double> kv) {
          return kv.getKey() + ": " + kv.getValue();
        }
      }))
      .apply(new WriteOneFilePerWindow(outputFilePath, 1));

    p.run().waitUntilFinish();
  }
}
