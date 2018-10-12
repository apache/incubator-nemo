/*
 * Copyright (C) 2018 Seoul National University
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.nemo.examples.beam;

import org.apache.beam.examples.common.WriteOneFilePerWindow;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.KV;
import org.apache.nemo.compiler.frontend.beam.NemoPipelineOptions;
import org.apache.nemo.compiler.frontend.beam.NemoPipelineRunner;
import org.joda.time.Duration;
import org.joda.time.Instant;

/**
 * A Windowed WordCount application.
 */
public final class WindowedWordCount {
  /**
   * Private Constructor.
   */
  private WindowedWordCount() {
  }

  /**
   * Main function for the MR BEAM program.
   * @param args arguments.
   */
  public static void main(final String[] args) {
    final String inputFilePath = args[0];
    final String outputFilePath = args[1];
    final PipelineOptions options = PipelineOptionsFactory.create().as(NemoPipelineOptions.class);
    options.setRunner(NemoPipelineRunner.class);
    options.setJobName("WindowedWordCount");

    final Pipeline p = Pipeline.create(options);
    GenericSourceSink.read(p, inputFilePath)
        .apply(ParDo.of(new DoFn<String, String>() {
            @ProcessElement
            public void processElement(@Element final String elem,
                                       final OutputReceiver<String> out) {
              final String[] splitt = elem.split("!");
              out.outputWithTimestamp(splitt[0], new Instant(Long.valueOf(splitt[1])));
            }
        }))
        .apply(Window.<String>into(FixedWindows.of(Duration.standardSeconds(5))))
        .apply(MapElements.<String, KV<String, Long>>via(new SimpleFunction<String, KV<String, Long>>() {
          @Override
          public KV<String, Long> apply(final String line) {
            final String[] words = line.split(" +");
            final String documentId = words[0] + "#" + words[1];
            final Long count = Long.parseLong(words[2]);
            return KV.of(documentId, count);
          }
        }))
        .apply(Sum.longsPerKey())
        .apply(MapElements.<KV<String, Long>, String>via(new SimpleFunction<KV<String, Long>, String>() {
          @Override
          public String apply(final KV<String, Long> kv) {
            return kv.getKey() + ": " + kv.getValue();
          }
        }))
        .apply(new WriteOneFilePerWindow(outputFilePath, null));
    p.run();
  }
}
