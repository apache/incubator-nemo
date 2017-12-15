/*
 * Copyright (C) 2017 Seoul National University
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
package edu.snu.onyx.examples.beam;

import edu.snu.onyx.client.beam.OnyxPipelineOptions;
import edu.snu.onyx.client.beam.OnyxPipelineRunner;
import edu.snu.onyx.examples.beam.common.WriteOneFilePerWindow;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.joda.time.Duration;
import org.joda.time.Instant;

import java.util.concurrent.ThreadLocalRandom;

/**
 * Sample MapReduce application.
 */
public final class WindowedMapReduce {
  static final int WINDOW_SIZE = 10;  // Default window duration in minutes

  /**
   * Private Constructor.
   */
  private WindowedMapReduce() {
  }

  /**
   * Each line of the word will get a random associated timestamp somewhere in a 1-hour period.
   */
  static class AddTimestampFn extends DoFn<String, String> {
    private final Instant minTimestamp;
    private final Instant maxTimestamp;

    AddTimestampFn(final Instant minTimestamp, final Instant maxTimestamp) {
      this.minTimestamp = minTimestamp;
      this.maxTimestamp = maxTimestamp;
    }

    @ProcessElement
    public void processElement(final ProcessContext c) {
      Instant randomTimestamp =
          new Instant(
              ThreadLocalRandom.current()
                  .nextLong(minTimestamp.getMillis(), maxTimestamp.getMillis()));

      /**
       * Concept #2: Set the data element with that timestamp.
       */
      c.outputWithTimestamp(c.element(), new Instant(randomTimestamp));
    }
  }

  /**
   * Main function for the MR BEAM program.
   * @param args arguments.
   */
  public static void main(final String[] args) {
    final String inputFilePath = args[0];
    final String outputFilePath = args[1];
    final PipelineOptions options = PipelineOptionsFactory.create().as(OnyxPipelineOptions.class);
    final Instant minTimestamp = new Instant(System.currentTimeMillis());
    final Instant maxTimestamp = new Instant(minTimestamp.getMillis() + Duration.standardHours(1).getMillis());

    options.setRunner(OnyxPipelineRunner.class);
    options.setJobName("MapReduce");

    final Pipeline p = Pipeline.create(options);
    final PCollection<String> input = GenericSourceSink.read(p, inputFilePath)
        .apply(ParDo.of(new AddTimestampFn(minTimestamp, maxTimestamp)));

    PCollection<String> windowedWords =
        input.apply(
            Window.<String>into(
                FixedWindows.of(Duration.standardMinutes(WINDOW_SIZE))));

    PCollection<String> wordCounts = windowedWords
        .apply(MapElements.<String, KV<String, Long>>via(new SimpleFunction<String, KV<String, Long>>() {
          @Override
          public KV<String, Long> apply(final String line) {
            final String[] words = line.split(" +");
            final String documentId = words[0] + "#" + words[1];
            final Long count = Long.parseLong(words[2]);
            return KV.of(documentId, count);
          }
        }))
        .apply(GroupByKey.<String, Long>create())
        .apply(Combine.<String, Long, Long>groupedValues(Sum.ofLongs()))
        .apply(MapElements.<KV<String, Long>, String>via(new SimpleFunction<KV<String, Long>, String>() {
          @Override
          public String apply(final KV<String, Long> kv) {
            return kv.getKey() + ": " + kv.getValue();
          }
        }));

    wordCounts.apply(new WriteOneFilePerWindow(outputFilePath));
    p.run();
  }
}
