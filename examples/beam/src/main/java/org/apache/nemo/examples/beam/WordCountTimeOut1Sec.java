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

import static org.apache.nemo.examples.beam.WordCount.generateWordCountPipeline;

/**
 * WordCount application, but with a timeout of 1 second.
 */
public final class WordCountTimeOut1Sec {

  /**
   * Private constructor.
   */
  private WordCountTimeOut1Sec() {
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
    options.setJobName("WordCountTimeOut1Sec");

    final Pipeline p = generateWordCountPipeline(options, inputFilePath, outputFilePath);
    p.run().waitUntilFinish(org.joda.time.Duration.standardSeconds(1));
  }
}
