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

import org.apache.nemo.compiler.frontend.beam.NemoPipelineOptions;
import org.apache.nemo.compiler.frontend.beam.NemoRunner;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.values.*;

import java.util.Arrays;

/**
 * Partition words by length example.
 */
public final class PartitionWordsByLength {
  /**
   * Private Constructor.
   */
  private PartitionWordsByLength() {
  }

  /**
   * Main function for the MR BEAM program.
   *
   * @param args arguments.
   */
  public static void main(final String[] args) {
    final String inputFilePath = args[0];
    final String outputFilePath = args[1];
    final PipelineOptions options = PipelineOptionsFactory.create().as(NemoPipelineOptions.class);
    options.setRunner(NemoRunner.class);
    options.setJobName("PartitionWordsByLength");

    // {} here is required for preserving type information.
    // Please see https://stackoverflow.com/a/48431397 for details.
    final TupleTag<KV<Integer, String>> shortWordsTag = new TupleTag<KV<Integer, String>>("short") {
    };
    final TupleTag<KV<Integer, String>> longWordsTag = new TupleTag<KV<Integer, String>>("long") {
    };
    final TupleTag<String> veryLongWordsTag = new TupleTag<String>("very long") {
    };
    final TupleTag<String> veryVeryLongWordsTag = new TupleTag<String>("very very long") {
    };

    final Pipeline p = Pipeline.create(options);
    final PCollection<String> lines = GenericSourceSink.read(p, inputFilePath);

    PCollectionTuple results = lines
        .apply(FlatMapElements
            .into(TypeDescriptors.strings())
            .via(line -> Arrays.asList(line.split(" "))))
        .apply(ParDo.of(new DoFn<String, String>() {
          // processElement with Beam OutputReceiver.
          @ProcessElement
          public void processElement(final ProcessContext c) {
            final String word = c.element();
            if (word.length() < 6) {
              c.output(shortWordsTag, KV.of(word.length(), word));
            } else if (word.length() < 11) {
              c.output(longWordsTag, KV.of(word.length(), word));
            } else if (word.length() > 12) {
              c.output(veryVeryLongWordsTag, word);
            } else {
              c.output(word);
            }
          }
        }).withOutputTags(veryLongWordsTag, TupleTagList
            .of(shortWordsTag).and(longWordsTag).and(veryVeryLongWordsTag)));

    PCollection<String> shortWords = results.get(shortWordsTag)
        .apply(GroupByKey.create())
        .apply(MapElements.via(new FormatLines()));
    PCollection<String> longWords = results.get(longWordsTag)
        .apply(GroupByKey.create())
        .apply(MapElements.via(new FormatLines()));
    PCollection<String> veryLongWords = results.get(veryLongWordsTag);
    PCollection<String> veryVeryLongWords = results.get(veryVeryLongWordsTag);

    GenericSourceSink.write(shortWords, outputFilePath + "_short");
    GenericSourceSink.write(longWords, outputFilePath + "_long");
    GenericSourceSink.write(veryLongWords, outputFilePath + "_very_long");
    GenericSourceSink.write(veryVeryLongWords, outputFilePath + "_very_very_long");
    p.run();
  }

  /**
   * Formats a key-value pair to a string.
   */
  static class FormatLines extends SimpleFunction<KV<Integer, Iterable<String>>, String> {
    @Override
    public String apply(final KV<Integer, Iterable<String>> input) {
      final int length = input.getKey();
      final StringBuilder sb = new StringBuilder();
      for (final String word : input.getValue()) {
        sb.append(length).append(": ").append(word).append('\n');
      }

      return sb.toString();
    }
  }
}
