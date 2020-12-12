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
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.VarIntCoder;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.SlidingWindows;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.joda.time.Duration;
import org.joda.time.Instant;

import java.io.Serializable;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Application for EDGAR dataset.
 * Format: ip, date, time, zone, doc_cik, access number, doc_name, code, size, idx, norefer, noagent, find, crawler.
 * Top 10 documents tha produce the most bad referer errors.
 */
public final class EDGARTop10BadRefererDocs {
  /**
   * Private Constructor.
   */
  private EDGARTop10BadRefererDocs() {
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

    final Window<KV<Object, Integer>> windowFn;
    if (windowType.equals("fixed")) {
      windowFn = Window.into(FixedWindows.of(Duration.standardSeconds(5)));
    } else {
      windowFn = Window.into(SlidingWindows.of(Duration.standardSeconds(10))
        .every(Duration.standardSeconds(5)));
    }

    final PipelineOptions options = NemoPipelineOptionsFactory.create();
    options.setJobName("EDGAR: Top 10 documents with bad referers");

    final Pipeline p = Pipeline.create(options);

    final PCollection<KV<Object, Integer>> source = GenericSourceSink.read(p, inputFilePath)
      .apply(ParDo.of(new DoFn<String, KV<Object, Integer>>() {
        @ProcessElement
        public void processElement(@DoFn.Element final String elem,
                                   final OutputReceiver<KV<Object, Integer>> out) {
          final String[] splitt = elem.split(",");
          final Integer failure = splitt[7].startsWith("2") ? 0 : 1;
          out.outputWithTimestamp(KV.of(splitt[6], failure), Instant.parse(splitt[1] + "T" + splitt[2] + "Z"));
        }
      }));
    source.setCoder(KvCoder.of(ObjectCoderForString.of(), VarIntCoder.of()));
    source.apply(windowFn)
      .apply(Mean.perKey())
      .apply(Top.of(10, new ValueComparator<>()).withoutDefaults())
      .apply(MapElements.via(new SimpleFunction<List<KV<Object, Double>>, String>() {
        @Override
        public String apply(final List<KV<Object, Double>> kvs) {
          return kvs.stream().map(kv -> kv.getKey() + ": " + kv.getValue()).collect(Collectors.joining("\n"));
        }
      }))
      .apply(new WriteOneFilePerWindow(outputFilePath, 1));

    p.run().waitUntilFinish();
  }

  /**
   * Value comparator comparing the double value.
   * @param <K> the key type.
   */
  public static final class ValueComparator<K> implements Comparator<KV<K, Double>>, Serializable {
    @Override
    public int compare(final KV<K, Double> o1, final KV<K, Double> o2) {
      return o1.getValue().compareTo(o2.getValue());
    }
  }
}
