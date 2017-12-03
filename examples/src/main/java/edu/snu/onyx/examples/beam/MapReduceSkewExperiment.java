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
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Sample MapReduce application.
 */
public final class MapReduceSkewExperiment {

  private static final Logger LOG = LoggerFactory.getLogger(MapReduce.class.getName());

  /**
   * Private Constructor.
   */
  private MapReduceSkewExperiment() {
  }

  /**
   * Main function for the MR BEAM program.
   * @param args arguments.
   */
  public static void main(final String[] args) {
    final String inputFilePath = args[0];
    final String outputFilePath = args[1];
    final PipelineOptions options = PipelineOptionsFactory.create().as(OnyxPipelineOptions.class);
    options.setRunner(OnyxPipelineRunner.class);
    options.setJobName("MapReduce");

    final Pipeline p = Pipeline.create(options);
    final PCollection<String> result = GenericSourceSink.read(p, inputFilePath)
        .apply(MapElements.<String, KV<String, Long>>via(new SimpleFunction<String, KV<String, Long>>() {
          @Override
          public KV<String, Long> apply(final String line) {
            final String[] words = line.split(" +");
            String network = "";
            String[] ip = words[1].split("\\.");
            if (ip.length == 1) {
              String[] ipv6 = words[1].split(":");
              if (ipv6.length == 1) {
                network = "192.168";
              } else {
                network = ipv6[0] + ":" + ipv6[1];
              }
            } else {
              network = ip[0] + "." + ip[1];
            }
            Long data = 1L;
            //LOG.info("Map#1 : ip {} network {} data_len {}", ip, network, data);
            return KV.of(network, data);
          }
        }))
        .apply(GroupByKey.<String, Long>create())
        .apply(Combine.<String, Long, Long>groupedValues(Sum.ofLongs()))
        .apply(MapElements.<KV<String, Long>, String>via(new SimpleFunction<KV<String, Long>, String>() {
          @Override
          public String apply(final KV<String, Long> kv) {
            //LOG.info("Map#2 : key {} value {}", kv.getKey(), kv.getValue());
            return kv.getKey() + ": " + kv.getValue();
          }
        }));
    GenericSourceSink.write(result, outputFilePath);

    p.run();
  }
}