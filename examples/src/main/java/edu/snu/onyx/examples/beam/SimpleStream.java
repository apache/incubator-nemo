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
import edu.snu.onyx.client.beam.OnyxPipelineResult;
import edu.snu.onyx.client.beam.OnyxPipelineRunner;
import edu.snu.onyx.examples.beam.common.UnboundedTextSource;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.Read;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Count;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.joda.time.Duration;

import java.io.IOException;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Simple stream.
 */
public final class SimpleStream {
  /**
   * Main function for the BEAM Stream program.
   */
  private SimpleStream() {
  }

  /**
   * Extract space separated words.
   */
  static class ExtractWordsFn extends DoFn<String, String> {
    @ProcessElement
    public void processElement(final ProcessContext c) {
      String[] words = c.element().split("[^a-zA-Z']+");

      for (String word : words) {
        if (!word.isEmpty()) {
          c.output(word);
        }
      }
    }
  }

  /**
   * Collects streaming pipeline results to memory.
   */
  static class CollectResultsFn extends DoFn<KV<String, Long>, String> {
    static final ConcurrentHashMap<String, Long> RESULTS = new ConcurrentHashMap<>();

    @ProcessElement
    public void processElement(final ProcessContext c) {
      RESULTS.put(c.element().getKey(), c.element().getValue());
    }
  }

  public static void main(final String[] args) throws IOException {
    final PipelineOptions options = PipelineOptionsFactory.create().as(OnyxPipelineOptions.class);
    options.setRunner(OnyxPipelineRunner.class);
    options.setJobName("SimpleStream");

    final Pipeline p = Pipeline.create(options);

    PCollection<KV<String, Long>> wordCounts =
        p.apply(Read.from(new UnboundedTextSource()))
        .apply(ParDo.of(new ExtractWordsFn()))
        .apply(Window.<String>into(FixedWindows.of(Duration.standardSeconds(10))))
        .apply(Count.<String>perElement());

    wordCounts.apply(ParDo.of(new CollectResultsFn()));

    final OnyxPipelineResult result = (OnyxPipelineResult) p.run();

    long timeout = System.currentTimeMillis() + 30000;
    while (System.currentTimeMillis() < timeout) {
      if (CollectResultsFn.RESULTS.containsKey("foo")
          && CollectResultsFn.RESULTS.containsKey("bar")) {
        break;
      }
      result.waitUntilFinish(Duration.millis(1000));
    }
    result.cancel();
    System.out.println("#####" + CollectResultsFn.RESULTS.toString());
    CollectResultsFn.RESULTS.clear();
  }
}
