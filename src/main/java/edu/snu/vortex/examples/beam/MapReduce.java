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
package edu.snu.vortex.examples.beam;

import edu.snu.vortex.compiler.frontend.beam.Runner;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.TypeDescriptors;

/**
 * Sample MapReduce application.
 */
public final class MapReduce {
  private MapReduce() {
  }

  public static void main(final String[] args) {
    final String inputFilePath = args[0];
    final String outputFilePath = args[1];
    final PipelineOptions options = PipelineOptionsFactory.create();
    options.setRunner(Runner.class);

    final Pipeline p = Pipeline.create(options);
    p.apply(TextIO.Read.from(inputFilePath))
        .apply(MapElements.via((String line) -> {
          final String[] words = line.split(" +");
          final String documentId = words[0];
          final Long count = Long.parseLong(words[1]);
          return KV.of(documentId, count);
        }).withOutputType(TypeDescriptors.kvs(TypeDescriptors.strings(), TypeDescriptors.longs())))
        .apply(GroupByKey.<String, Long>create())
        .apply(Combine.<String, Long, Long>groupedValues(new Sum.SumLongFn()))
        .apply(MapElements.via((KV<String, Long> kv) -> kv.getKey() + ": " + kv.getValue())
            .withOutputType(TypeDescriptors.strings()))
        .apply(TextIO.Write.to(outputFilePath));
    p.run();
  }
}
