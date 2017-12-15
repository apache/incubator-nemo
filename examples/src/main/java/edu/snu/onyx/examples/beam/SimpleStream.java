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
import org.apache.beam.runners.apex.examples.UnboundedTextSource;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.Read;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.beam.sdk.values.PCollection;

public final class SimpleStream {

  private SimpleStream() {
  }

  /**
   * Main function for the Stream BEAM program.
   * @param args arguments.
   */
  public static void main(final String[] args) {
    final String inputFilePath = args[0];
    final String outputFilePath = args[1];
    final PipelineOptions options = PipelineOptionsFactory.create().as(OnyxPipelineOptions.class);
    options.setRunner(OnyxPipelineRunner.class);
    options.setJobName("SimpleStream");

    final Pipeline p = Pipeline.create(options);
    final PCollection<String> result = p.apply(Read.from(new UnboundedTextSource()))
        .apply(MapElements.<String, String>via(new SimpleFunction<String, String>() {
          @Override
          public String apply(final String line) {
            return line;
          }
        }));
    GenericSourceSink.write(result, outputFilePath);
    p.run();
  }
}
