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
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.beam.sdk.transforms.Sum;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;

/**
 * WordCount application.
 */
public final class WordCount {
  /**
   * Private Constructor.
   */
  private WordCount() {
  }

  /**
   * Main function for the MR BEAM program.
   *
   * @param args arguments.
   */
  public static void main(final String[] args) {
    final String inputFilePath = args[0];
    final String outputFilePath = args[1];
    final PipelineOptions options = NemoPipelineOptionsFactory.create();
    options.setJobName("WordCount");

    final Pipeline p = generateWordCountPipeline(options, inputFilePath, outputFilePath);
    p.run().waitUntilFinish();
  }

  /**
   * Static method to generate the word count Beam pipeline.
   * @param options options for the pipeline.
   * @param inputFilePath the input file path.
   * @param outputFilePath the output file path.
   * @return the generated pipeline.
   */
  static Pipeline generateWordCountPipeline(final PipelineOptions options,
                                            final String inputFilePath, final String outputFilePath) {
    final Pipeline p = Pipeline.create(options);
    final PCollection<String> result = GenericSourceSink.read(p, inputFilePath)
      .apply(MapElements.<String, KV<String, Long>>via(new SimpleFunction<String, KV<String, Long>>() {
        @Override
        public KV<String, Long> apply(final String line) {
          final String[] words = line.split(" +");
          final String documentId = words[0] + "#" + words[1];
          final Long count = Long.parseLong(words[2]);
          return KV.of(documentId, count);
        }
      }))
      .apply("work stealing", Sum.longsPerKey())
      .apply("merge", Sum.longsPerKey())
      .apply(MapElements.<KV<String, Long>, String>via(new SimpleFunction<KV<String, Long>, String>() {
        @Override
        public String apply(final KV<String, Long> kv) {
          return kv.getKey() + ": " + kv.getValue();
        }
      }));
    GenericSourceSink.write(result, outputFilePath);
    return p;
  }
}
