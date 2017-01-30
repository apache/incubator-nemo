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

import com.google.common.collect.ImmutableMap;
import edu.snu.vortex.compiler.frontend.beam.Runner;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.coders.BigEndianLongCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.io.kafka.KafkaIO;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.joda.time.Duration;

import java.io.IOException;
import java.util.Arrays;
import java.util.Map;
import java.util.Properties;


public class KafkaMapReduce {
  private static final int WINDOW_SIZE = 1;  // Default window duration in seconds

  private static String KAFKA_SERVER;
  private static String KAFKA_TOPIC;

  private static void produce(String topic, Map<String, String> messages) {
    Serializer<String> stringSerializer = new StringSerializer();
    try (@SuppressWarnings("unchecked") KafkaProducer<String, String> kafkaProducer =
             new KafkaProducer(producerProps(), stringSerializer, stringSerializer)) {
      // feed topic.
      for (Map.Entry<String, String> en : messages.entrySet()) {
        kafkaProducer.send(new ProducerRecord<>(topic, en.getKey(), en.getValue()));
      }
      // await send completion.
      kafkaProducer.flush();
    }
  }

  private static Properties producerProps() {
    Properties producerProps = new Properties();
    producerProps.put("acks", "1");
    producerProps.put("bootstrap.servers", Arrays.asList(KAFKA_SERVER));
    return producerProps;
  }



  public static void main(String[] args) throws IOException {
    KAFKA_SERVER = args[0];
    KAFKA_TOPIC = args[1];

    // messages.
    final Map<String, String> messages = ImmutableMap.of(
        "k1", "v1", "k2", "v2", "k3", "v3", "k4", "v4"
    );
    produce(KAFKA_TOPIC, messages);


    final PipelineOptions options;
    options = PipelineOptionsFactory.create();
    options.setRunner(Runner.class);

    final Duration windowSize = Duration.standardSeconds(WINDOW_SIZE);



    System.out.println(options);
    final Pipeline pipeline = Pipeline.create(options);


    /*
    final Map<String, Object> consumerProps = ImmutableMap.<String, Object>of(
        "auto.offset.reset", "earliest"
    );
    */

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
    final PCollection<KV<String, Long>> wordCounts = windowedWords.apply(MapElements.via((String line) -> {
          final String[] words = line.split(" +");
          final String documentId = words[0];
          final Long count = Long.parseLong(words[1]);
          return KV.of(documentId, count);
        }).withOutputType(TypeDescriptors.kvs(TypeDescriptors.strings(), TypeDescriptors.longs())))
        .apply(GroupByKey.<String, Long>create())
        .apply(Combine.<String, Long, Long>groupedValues(new Sum.SumLongFn()));

    /*
    wordCounts.apply(KafkaIO.write()
        .withBootstrapServers("localhost:9092")
        .withTopic("output")
        .withKeyCoder(StringUtf8Coder.of())
        .withValueCoder(BigEndianLongCoder.of()));
        */

    PipelineResult result = pipeline.run();
  }
}
