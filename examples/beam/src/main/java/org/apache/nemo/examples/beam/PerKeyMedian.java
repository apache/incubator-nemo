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

import com.google.common.collect.Lists;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.transforms.GroupByKey;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.List;

/**
 * IP stat analysis example used in skew experiment.
 */
public final class PerKeyMedian {
  private static final Logger LOG = LoggerFactory.getLogger(PerKeyMedian.class.getName());

  /**
   * Private Constructor.
   */
  private PerKeyMedian() {
  }

  /**
   * Main function for the MR BEAM program.
   *
   * @param args arguments.
   */
  public static void main(final String[] args) {
    final String inputFilePath = args[0];
    final String outputFilePath = args[1];
    final PipelineOptions options = NemoPipelineOptionsFactory.create();
    options.setJobName("PerKeyMedian");

    final Pipeline p = Pipeline.create(options);

    long start = System.currentTimeMillis();

    final PCollection<String> result = GenericSourceSink.read(p, inputFilePath)
      .apply(MapElements.via(new SimpleFunction<String, KV<String, String>>() {
        @Override
        public KV<String, String> apply(final String line) {
          final String[] words = line.split(" ");
          String key = words[0];
          String value = words[1];
          return KV.of(key, value);
        }
      }))
      .apply(GroupByKey.create())
      .apply(MapElements.via(new SimpleFunction<KV<String, Iterable<String>>, String>() {
        @Override
        public String apply(final KV<String, Iterable<String>> kv) {
          final String key = kv.getKey();
          List value = Lists.newArrayList(kv.getValue());
          Collections.sort(value);
          return key + ", " + value.get(value.size() / 2);
        }
      }));
    GenericSourceSink.write(result, outputFilePath);
    p.run().waitUntilFinish();

    LOG.info("*******END*******");
    LOG.info("JCT(ms): " + (System.currentTimeMillis() - start));
  }
}
