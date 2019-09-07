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
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.View;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionView;

import java.util.Optional;
import java.util.stream.StreamSupport;

/**
 * Sample Broadcast application.
 */
public final class Broadcast {
  /**
   * Private constructor.
   */
  private Broadcast() {
  }

  /**
   * Main function for the BEAM program.
   *
   * @param args arguments.
   */
  public static void main(final String[] args) {
    final String inputFilePath = args[0];
    final String outputFilePath = args[1];
    final PipelineOptions options = NemoPipelineOptionsFactory.create();

    final Pipeline p = Pipeline.create(options);
    final PCollection<String> elemCollection = GenericSourceSink.read(p, inputFilePath);
    final PCollectionView<Iterable<String>> allCollection = elemCollection.apply(View.<String>asIterable());

    final PCollection<String> result = elemCollection.apply(ParDo.of(new DoFn<String, String>() {
        @ProcessElement
        public void processElement(final ProcessContext c) {
          final String line = c.element();
          final Iterable<String> all = c.sideInput(allCollection);
          final Optional<String> appended = StreamSupport.stream(all.spliterator(), false)
            .reduce((l, r) -> l + '\n' + r);
          if (appended.isPresent()) {
            c.output("line: " + line + "\n" + appended.get());
          } else {
            c.output("error");
          }
        }
      }).withSideInputs(allCollection)
    );

    GenericSourceSink.write(result, outputFilePath);
    p.run().waitUntilFinish();
  }
}
