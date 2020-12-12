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
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.SlidingWindows;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.joda.time.Duration;
import org.joda.time.Instant;

import java.util.Random;

/**
 * A Windowed WordCount application.
 */
public final class WindowedWordCount {
  /**
   * Private Constructor.
   */
  private WindowedWordCount() {
  }

  public static final String INPUT_TYPE_BOUNDED = "bounded";
  public static final String INPUT_TYPE_UNBOUNDED = "unbounded";
  private static final String SPLITTER = "!";
  public static final Random RAND = new Random();

  /**
   * @param p    pipeline.
   * @param inputType input type arg.
   * @param inputFilePath input file path arg.
   * @return source.
   */
  private static PCollection<KV<String, Long>> getSource(
    final Pipeline p,
    final String inputType,
    final String inputFilePath) {

    if (inputType.compareTo(INPUT_TYPE_BOUNDED) == 0) {
      return GenericSourceSink.read(p, inputFilePath)
        .apply(ParDo.of(new DoFn<String, String>() {
          @ProcessElement
          public void processElement(@Element final String elem,
                                     final OutputReceiver<String> out) {
            final String[] splitt = elem.split(SPLITTER);
            if (splitt.length > 1 && splitt[1].matches("[0-9]+")) {
              out.outputWithTimestamp(splitt[0], new Instant(Long.valueOf(splitt[1])));
            } else {
              final long timestamp = System.currentTimeMillis() - RAND.nextInt(1000000);
              out.outputWithTimestamp(elem, new Instant(timestamp));
            }
          }
        }))
        .apply(MapElements.<String, KV<String, Long>>via(new SimpleFunction<String, KV<String, Long>>() {
          @Override
          public KV<String, Long> apply(final String line) {
            final String[] words = line.split(" +");
            final String documentId = words[0];
            if (words.length > 2) {
              final Long count = Long.parseLong(words[2]);
              return KV.of(documentId, count);
            } else {
              return KV.of(documentId, 1L);
            }
          }
        }));
    } else if (inputType.compareTo(INPUT_TYPE_UNBOUNDED) == 0) {
      // unbounded
      return p.apply(GenerateSequence
        .from(1)
        .withRate(2, Duration.standardSeconds(1))
        .withTimestampFn(num -> new Instant(num * 500))) // 0.5 second between subsequent elements
        .apply(MapElements.via(new SimpleFunction<Long, KV<String, Long>>() {
          @Override
          public KV<String, Long> apply(final Long val) {
            return KV.of(String.valueOf(val % 2), 1L);
          }
        }));
    } else {
      throw new RuntimeException("Unsupported input type: " + inputType);
    }
  }

  /**
   * Main function for the MR BEAM program.
   *
   * @param args arguments.
   */
  public static void main(final String[] args) {
    final String outputFilePath = args[0];
    final String windowType = args[1];
    final String inputType = args[2];
    final String inputFilePath = args[3];

    final Window<KV<String, Long>> windowFn;
    if (windowType.equals("fixed")) {
      windowFn = Window.<KV<String, Long>>into(FixedWindows.of(Duration.standardSeconds(5)));
    } else {
      windowFn = Window.<KV<String, Long>>into(SlidingWindows.of(Duration.standardSeconds(10))
        .every(Duration.standardSeconds(5)));
    }

    final PipelineOptions options = NemoPipelineOptionsFactory.create();
    options.setJobName("WindowedWordCount");

    final Pipeline p = Pipeline.create(options);

    getSource(p, inputType, inputFilePath)
      .apply(windowFn)
      .apply(Sum.longsPerKey())
      .apply(MapElements.<KV<String, Long>, String>via(new SimpleFunction<KV<String, Long>, String>() {
        @Override
        public String apply(final KV<String, Long> kv) {
          return kv.getKey() + ": " + kv.getValue();
        }
      }))
      .apply(new WriteOneFilePerWindow(outputFilePath, 1));

    p.run().waitUntilFinish();
  }
}
