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
import org.apache.commons.lang3.StringUtils;

import java.util.Collections;
import java.util.Random;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Sample MapReduce application for skew experiment.
 */
public final class MapReduceSkew {

  private static final Logger LOG = LoggerFactory.getLogger(MapReduceSkew.class.getName());

  /**
   * Private Constructor.
   */
  private MapReduceSkew() {
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
    options.setJobName("MapReduceSkew");

    final Pipeline p = Pipeline.create(options);

    long start = System.currentTimeMillis();

    final PCollection<String> result = GenericSourceSink.read(p, inputFilePath)
        .apply(MapElements.<String, KV<String, Integer>>via(new SimpleFunction<String, KV<String, Integer>>() {
          @Override
          public KV<String, Integer> apply(final String line) {
            final String[] words = line.split(" +");
            String key = parseIPtoNetwork(words[1]);
            Integer value = 1; //assignValue(words[5]);
            return KV.of(key, value);
          }
        }))
        .apply(GroupByKey.<String, Integer>create())
        .apply(Combine.<String, Integer, Integer>groupedValues(Sum.ofIntegers()))
        .apply(MapElements.<KV<String, Integer>, String>via(new SimpleFunction<KV<String, Integer>, String>() {
          @Override
          public String apply(final KV<String, Integer> kv) {
            //LOG.info("Map#2 : key {} value {}", kv.getKey(), kv.getValue());
            return kv.getKey() + ": " + kv.getValue();
          }
        }));
    GenericSourceSink.write(result, outputFilePath);

    p.run();

    LOG.info("*******END*******");
    LOG.info("JCT(ms): " + (System.currentTimeMillis() - start));
  }

  /**
   * Assign synthetic skewed keys.
   */
  private static String assignSkewedKey() {
    Random r = new Random();
    int p = r.nextInt(10);
    if (p == 0) {
      return String.join("", Collections.nCopies(20, "192.168"));
    } else if (p == 1) {
      return String.join("", Collections.nCopies(25, "206.51"));
    } else {
      return String.join("", Collections.nCopies(20, "43.252"));
    }
  }

  /**
   * Convert the length of a packet from network trace(tcpdump format) to integer.
   * @param value length of a packet.
   */
  private static Integer assignValue(final String value) {
    Random r = new Random();
    if (StringUtils.isNumeric(value)) {
      Integer num = Integer.parseInt(value);
      if (num > 100) {
        return r.nextInt(10);
      } else {
        return num;
      }
    } else {
      return r.nextInt(10);
    }
  }

  /**
   * Extract key as a network part of an IP address.
   * @param ip IP address of a packet.
   */
  private static String parseIPtoNetwork(final String ip) {
    String network = "";
    String[] ipparts = ip.split("\\.");
    if (ipparts.length == 1) {
      String[] ipv6 = ip.split(":");
      if (ipv6.length == 1) {
        network = "192.168";
      } else {
        network = ipv6[0] + ":" + ipv6[1];
      }
    } else {
      network = ipparts[0] + "." + ipparts[1];
    }
    return network;
  }
}
