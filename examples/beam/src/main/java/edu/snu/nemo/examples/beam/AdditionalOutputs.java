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
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.FlatMapElements;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.*;

import java.util.Arrays;

/**
 * Additional Tagged Outputs application.
 */
public final class AdditionalOutputs {
  /**
   * Private Constructor.
   */
  private AdditionalOutputs() {
  }

  /**
   * Main function for the MR BEAM program.
   *
   * @param args arguments.
   */
  public static void main(final String[] args) {
    final String inputFilePath = args[0];
    final String outputFilePath = args[1];
    final PipelineOptions options = PipelineOptionsFactory.create().as(NemoPipelineOptions.class);
    options.setRunner(NemoPipelineRunner.class);
    options.setJobName("AdditionalOutputs");

    final TupleTag<String> shortWordsTag = new TupleTag<String>() {
    };
    final TupleTag<Integer> longWordsTag = new TupleTag<Integer>() {
    };
    final TupleTag<String> veryLongWordsTag = new TupleTag<String>() {
    };

    final Pipeline p = Pipeline.create(options);
    final PCollection<String> lines = GenericSourceSink.read(p, inputFilePath);

    PCollectionTuple results = lines
        .apply(FlatMapElements
            .into(TypeDescriptors.strings())
            .via(line -> Arrays.asList(line.split(" "))))
        .apply(ParDo.of(new DoFn<String, String>() {
          @ProcessElement
          public void processElement(final ProcessContext c) {
            String word = c.element();
            if (word.length() < 5) {
              c.output(shortWordsTag, word);
            } else if (word.length() < 8) {
              c.output(longWordsTag, word.length());
            } else {
              c.output(veryLongWordsTag, word);
            }
          }
        }).withOutputTags(veryLongWordsTag, TupleTagList
            .of(longWordsTag)
            .and(shortWordsTag)));

    PCollection<String> shortWords = results.get(shortWordsTag);
    PCollection<String> longWordLengths = results
        .get(longWordsTag)
        .apply(MapElements.into(TypeDescriptors.strings()).via(i -> Integer.toString(i)));
    PCollection<String> veryLongWords = results.get(veryLongWordsTag);

    GenericSourceSink.write(shortWords, outputFilePath + "_short");
    GenericSourceSink.write(longWordLengths, outputFilePath + "_long");
    GenericSourceSink.write(veryLongWords, outputFilePath + "_very_long");
    p.run();
  }
}
