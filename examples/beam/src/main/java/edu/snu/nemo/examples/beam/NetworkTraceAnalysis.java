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
package edu.snu.nemo.examples.beam;

import edu.snu.nemo.compiler.frontend.beam.NemoPipelineOptions;
import edu.snu.nemo.compiler.frontend.beam.NemoPipelineRunner;
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

import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * An app that analyzes data flow from network trace.
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
    options.setRunner(NemoPipelineRunner.class);
    options.setJobName("NetworkTraceAnalysis");

    final Pattern pattern = Pattern.compile(" *\\d+ +[0-9.]+ +([0-9.]+) -> ([0-9.]+) +.*Len=(\\d+)");

    final SimpleFunction<String, Boolean> filter = new SimpleFunction<String, Boolean>() {
      @Override
      public Boolean apply(final String line) {
        return pattern.matcher(line).find();
      }
    };
    final SimpleFunction<String, KV<String, KV<String, Long>>> mapToMatchedGroup
        = new SimpleFunction<String, KV<String, KV<String, Long>>>() {
      @Override
      public KV<String, KV<String, Long>> apply(final String line) {
        final Matcher matcher = pattern.matcher(line);
        matcher.find();
        return KV.of(matcher.group(2), KV.of(matcher.group(1), Long.valueOf(matcher.group(3))));
      }
    };
    final SimpleFunction<KV<String, Iterable<KV<String, Long>>>, KV<String, Double>> mapToStdev
        = new SimpleFunction<KV<String, Iterable<KV<String, Long>>>, KV<String, Double>>() {
      @Override
      public KV<String, Double> apply(final KV<String, Iterable<KV<String, Long>>> kv) {
        return KV.of(kv.getKey(), stdev(kv.getValue()));
      }
    };

    final Pipeline p = Pipeline.create(options);
    final PCollection<KV<String, Double>> in0 = GenericSourceSink.read(p, input0FilePath)
        .apply(Filter.by(filter))
        .apply(MapElements.via(mapToMatchedGroup))
        .apply(GroupByKey.create())
        .apply(MapElements.via(mapToStdev));
    final PCollection<KV<String, Double>> in1 = GenericSourceSink.read(p, input1FilePath)
        .apply(Filter.by(filter))
        .apply(MapElements.via(mapToMatchedGroup))
        .apply(GroupByKey.create())
        .apply(MapElements.via(mapToStdev));
    final TupleTag<Double> tag0 = new TupleTag<>();
    final TupleTag<Double> tag1 = new TupleTag<>();
    final PCollection<KV<String, CoGbkResult>> joined =
        KeyedPCollectionTuple.of(tag0, in0).and(tag1, in1).apply(CoGroupByKey.create());
    final PCollection<String> result = joined
        .apply(MapElements.via(new SimpleFunction<KV<String, CoGbkResult>, String>() {
          @Override
          public String apply(final KV<String, CoGbkResult> kv) {
            final double source = getDouble(kv.getValue().getAll(tag0));
            final double destination = getDouble(kv.getValue().getAll(tag1));
            final String intermediate = kv.getKey();
            return new StringBuilder(intermediate).append(",").append(source).append(",")
                .append(destination).toString();
          }
        }));
    GenericSourceSink.write(result, outputFilePath);
    p.run();
  }

  private static double getDouble(final Iterable<Double> data) {
    for (final double datum : data) {
      return datum;
    }
    return Double.NaN;
  }

  private static double stdev(final Iterable<KV<String, Long>> data) {
    long num = 0;
    long sum = 0;
    long squareSum = 0;
    for (final KV<String, Long> e : data) {
      final long element = e.getValue();
      num++;
      sum += element;
      squareSum += (element * element);
    }
    if (num == 0) {
      return Double.NaN;
    }
    final double average = ((double) sum) / num;
    final double squareAverage = ((double) squareSum) / num;
    return Math.sqrt(squareAverage - average * average);
  }
}
