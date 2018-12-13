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
import org.apache.beam.sdk.transforms.join.CoGbkResult;
import org.apache.beam.sdk.transforms.join.CoGroupByKey;
import org.apache.beam.sdk.transforms.join.KeyedPCollectionTuple;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.commons.math3.stat.descriptive.moment.StandardDeviation;

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * An app that analyzes data flow from network trace.
 * Each line in the output file represents a host, containing the standard deviation of the lengths of packets
 * that flows into the host (reads input0 file), and the standard deviation of the lengths of packets
 * that flows out from the host (reads input1 file).
 */
public final class NetworkTraceAnalysis {
  /**
   * Private constructor.
   */
  private NetworkTraceAnalysis() {
  }

  /**
   * Main function for the Beam program.
   * @param args arguments.
   */
  public static void main(final String[] args) {
    final String input0FilePath = args[0];
    final String input1FilePath = args[1];
    final String outputFilePath = args[2];
    final PipelineOptions options = PipelineOptionsFactory.create().as(NemoPipelineOptions.class);
    options.setRunner(NemoRunner.class);
    options.setJobName("NetworkTraceAnalysis");

    // Given "4 0.0 192.168.3.1 -> 192.168.0.2 Len=29", this finds "192.168.3.1", "192.168.0.2" and "29"
    final Pattern pattern = Pattern.compile(" *\\d+ +[0-9.]+ +([0-9.]+) -> ([0-9.]+) +.*Len=(\\d+)");

    final SimpleFunction<String, Boolean> filter = new SimpleFunction<String, Boolean>() {
      @Override
      public Boolean apply(final String line) {
        return pattern.matcher(line).find();
      }
    };
    final SimpleFunction<KV<String, Iterable<KV<String, Long>>>, KV<String, Long>> mapToStdev
        = new SimpleFunction<KV<String, Iterable<KV<String, Long>>>, KV<String, Long>>() {
      @Override
      public KV<String, Long> apply(final KV<String, Iterable<KV<String, Long>>> kv) {
        return KV.of(kv.getKey(), stdev(kv.getValue()));
      }
    };

    final Pipeline p = Pipeline.create(options);
    final PCollection<KV<String, Long>> in0 = GenericSourceSink.read(p, input0FilePath)
        .apply(Filter.by(filter))
        .apply(MapElements.via(new SimpleFunction<String, KV<String, KV<String, Long>>>() {
          @Override
          public KV<String, KV<String, Long>> apply(final String line) {
            final Matcher matcher = pattern.matcher(line);
            matcher.find();
            return KV.of(matcher.group(2), KV.of(matcher.group(1), Long.valueOf(matcher.group(3))));
          }
        }))
        .apply(GroupByKey.create())
        .apply(MapElements.via(mapToStdev));
    final PCollection<KV<String, Long>> in1 = GenericSourceSink.read(p, input1FilePath)
        .apply(Filter.by(filter))
        .apply(MapElements.via(new SimpleFunction<String, KV<String, KV<String, Long>>>() {
          @Override
          public KV<String, KV<String, Long>> apply(final String line) {
            final Matcher matcher = pattern.matcher(line);
            matcher.find();
            return KV.of(matcher.group(1), KV.of(matcher.group(2), Long.valueOf(matcher.group(3))));
          }
        }))
        .apply(GroupByKey.create())
        .apply(MapElements.via(mapToStdev));
    final TupleTag<Long> tag0 = new TupleTag<>();
    final TupleTag<Long> tag1 = new TupleTag<>();
    final PCollection<KV<String, CoGbkResult>> joined =
        KeyedPCollectionTuple.of(tag0, in0).and(tag1, in1).apply(CoGroupByKey.create());
    final PCollection<String> result = joined
        .apply(MapElements.via(new SimpleFunction<KV<String, CoGbkResult>, String>() {
          @Override
          public String apply(final KV<String, CoGbkResult> kv) {
            final long source = getLong(kv.getValue().getAll(tag0));
            final long destination = getLong(kv.getValue().getAll(tag1));
            final String intermediate = kv.getKey();
            return new StringBuilder(intermediate).append(",").append(source).append(",")
                .append(destination).toString();
          }
        }));
    GenericSourceSink.write(result, outputFilePath);
    p.run();
  }

  /**
   * @param data data
   * @return extracted long typed data
   */
  private static long getLong(final Iterable<Long> data) {
    for (final long datum : data) {
      return datum;
    }
    return 0;
  }

  /**
   * @param data list of data
   * @return standard deviation of data.
   */
  private static long stdev(final Iterable<KV<String, Long>> data) {
    final StandardDeviation stdev = new StandardDeviation();
    final List<Long> elements = new ArrayList<>();
    for (final KV<String, Long> e : data) {
      elements.add(e.getValue());
    }
    return Math.round(stdev.evaluate(elements.stream().mapToDouble(e -> e).toArray()));
  }
}
