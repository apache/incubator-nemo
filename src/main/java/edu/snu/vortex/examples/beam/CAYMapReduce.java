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
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.io.Write;
import org.apache.beam.sdk.io.hdfs.HDFSFileSink;
import org.apache.beam.sdk.io.hdfs.WritableCoder;
import org.apache.beam.sdk.io.kafka.KafkaIO;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.joda.time.Duration;

import java.io.IOException;
import java.util.Arrays;


public class CAYMapReduce {
  public static class FormatForHDFS extends SimpleFunction<KV<String, Long>, KV<Text, LongWritable>> {
    @Override
    public KV<Text, LongWritable> apply(KV<String, Long> kv) {
      return KV.of(new Text(kv.getKey()), new LongWritable(kv.getValue()));
    }
  }


  public static void main(String[] args) throws IOException {
    final String KAFKA_SERVER = args[0];
    final String KAFKA_TOPIC = args[1];
    final Duration windowSize = Duration.standardSeconds(Integer.valueOf(args[2]));
    final String HDFS_PATH = args[3];

    final PipelineOptions options;
    options = PipelineOptionsFactory.create();
    options.setRunner(Runner.class);
    final Pipeline pipeline = Pipeline.create(options);

    final PCollection<String> input = pipeline
        .apply(KafkaIO.read()
            .withBootstrapServers(KAFKA_SERVER)
            .withTopics(Arrays.asList(KAFKA_TOPIC))
            .withKeyCoder(StringUtf8Coder.of())
            .withValueCoder(StringUtf8Coder.of())
            //.updateConsumerProperties(consumerProps)
            .withoutMetadata())
        //.setCoder(KvCoder.of(StringUtf8Coder.of(), StringUtf8Coder.of()))
        .apply(Values.<String>create());

    final PCollection<String> windowedWords = input.apply(Window.<String>into(FixedWindows.of(windowSize)));
    final PCollection<KV<String, Long>> wordCounts = windowedWords
        .apply(MapElements.via((String line) -> {
          final String[] words = line.split(" ");
          if (words.length < 4) {
            System.out.println("line: " + line);
            Arrays.stream(words).forEach(w -> System.out.println(w));
            System.out.println("done");
          }
          final String documentId = words[0];
          final Long count = Long.parseLong(words[3]);
          return KV.of(documentId, count);
        }).withOutputType(TypeDescriptors.kvs(TypeDescriptors.strings(), TypeDescriptors.longs())))
        .apply(GroupByKey.<String, Long>create())
        .apply(Combine.<String, Long, Long>groupedValues(new Sum.SumLongFn()));

    final PCollection<KV<Text, LongWritable>> forHDFS = wordCounts.apply(MapElements.via(new FormatForHDFS()))
        .setCoder(KvCoder.of(WritableCoder.of(Text.class), WritableCoder.of(LongWritable.class)));

    // Write!
    forHDFS.apply(Write.to(new HDFSFileSink(HDFS_PATH, new TextOutputFormat<Text, LongWritable>().getClass())));

    PipelineResult result = pipeline.run();
  }



}
