/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package edu.snu.vortex.examples.beam;

import edu.snu.vortex.compiler.frontend.beam.Runner;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.*;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.IntervalWindow;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.joda.time.Duration;
import org.joda.time.Instant;

import java.io.IOException;
import java.util.concurrent.ThreadLocalRandom;


public class WindowedMapReduce {
    static final int WINDOW_SIZE = 10;  // Default window duration in minutes
  /**
   * Concept #2: A DoFn that sets the data element timestamp. This is a silly method, just for
   * this example, for the bounded data case.
   *
   * <p>Imagine that many ghosts of Shakespeare are all typing madly at the same time to recreate
   * his masterworks. Each line of the corpus will get a random associated timestamp somewhere in a
   * 2-hour period.
   */
  static class AddTimestampFn extends DoFn<String, String> {
    private static final Duration RAND_RANGE = Duration.standardHours(1);
    private final Instant minTimestamp;
    private final Instant maxTimestamp;

    AddTimestampFn(Instant minTimestamp, Instant maxTimestamp) {
      this.minTimestamp = minTimestamp;
      this.maxTimestamp = maxTimestamp;
    }

    @ProcessElement
    public void processElement(ProcessContext c) {
      Instant randomTimestamp =
          new Instant(
              ThreadLocalRandom.current()
                  .nextLong(minTimestamp.getMillis(), maxTimestamp.getMillis()));
      c.outputWithTimestamp(c.element(), new Instant(randomTimestamp));
    }
  }

  public static void main(String[] args) throws IOException {
    final String inputFilePath = args[0];
    final String outputFilePath = args[1];
    final PipelineOptions options = PipelineOptionsFactory.create();
    options.setRunner(Runner.class);

    final Duration windowSize = Duration.standardMinutes(WINDOW_SIZE);
    final Instant minTimestamp = new Instant(System.currentTimeMillis());
    final Instant maxTimestamp = new Instant(System.currentTimeMillis() + Duration.standardHours(1).getMillis());

    final Pipeline pipeline = Pipeline.create(options);

    // IMPORTANT: save timestamp (outputWithTimestamp())
    final PCollection<String> input = pipeline
      .apply(TextIO.Read.from(inputFilePath))
      .apply(ParDo.of(new AddTimestampFn(minTimestamp, maxTimestamp)));

    // IMPORTANT: convert timestamp (timestamp()) into window
    final PCollection<String> windowedWords = input.apply(Window.<String>into(FixedWindows.of(windowSize)));

    // Same old stuff
    final PCollection<KV<String, Long>> wordCounts = windowedWords.apply(MapElements.via((String line) -> {
          final String[] words = line.split(" +");
          final String documentId = words[0];
          final Long count = Long.parseLong(words[1]);
          return KV.of(documentId, count);
        }).withOutputType(TypeDescriptors.kvs(TypeDescriptors.strings(), TypeDescriptors.longs())))
        .apply(GroupByKey.<String, Long>create())
        .apply(Combine.<String, Long, Long>groupedValues(new Sum.SumLongFn()));

    // IMPORTANT: get window through ProcessContext#window (window())
    final PCollection<KV<IntervalWindow, KV<String, Long>>> keyedByWindow =
        wordCounts.apply(
            ParDo.of(
                new DoFn<KV<String, Long>, KV<IntervalWindow, KV<String, Long>>>() {
                  @ProcessElement
                  public void processElement(ProcessContext context, IntervalWindow window) {
                    context.output(KV.of(window, context.element()));
                  }
                }));

    /*
    keyedByWindow
        .apply(GroupByKey.<IntervalWindow, KV<String, Long>>create())
        .apply(ParDo.of(new WriteWindowedFilesDoFn(output)));
        */

    PipelineResult result = pipeline.run();
  }
}
