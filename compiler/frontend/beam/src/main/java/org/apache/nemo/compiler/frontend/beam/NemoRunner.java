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
package org.apache.nemo.compiler.frontend.beam;

import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.nemo.client.JobLauncher;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineRunner;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsValidator;

/**
 * Runner class for BEAM programs.
 */
public final class NemoRunner extends PipelineRunner<NemoPipelineResult> {
  private final NemoPipelineOptions nemoPipelineOptions;

  /**
   * BEAM Pipeline Runner.
   * @param nemoPipelineOptions PipelineOptions.
   */
  private NemoRunner(final NemoPipelineOptions nemoPipelineOptions) {
    this.nemoPipelineOptions = nemoPipelineOptions;
  }

  /**
   * Creates and returns a new NemoRunner with default options.
   *
   * @return A pipeline runner with default options.
   */
  public static NemoRunner create() {
    NemoPipelineOptions options = PipelineOptionsFactory.as(NemoPipelineOptions.class);
    options.setRunner(NemoRunner.class);
    return new NemoRunner(options);
  }

  /**
   * Creates and returns a new NemoRunner with specified options.
   *
   * @param options The NemoPipelineOptions to use when executing the job.
   * @return A pipeline runner that will execute with specified options.
   */
  public static NemoRunner create(final NemoPipelineOptions options) {
    return new NemoRunner(options);
  }

  /**
   * Static initializer for creating PipelineRunner with the given options.
   * @param options given PipelineOptions.
   * @return The created PipelineRunner.
   */
  public static NemoRunner fromOptions(final PipelineOptions options) {
    final NemoPipelineOptions nemoOptions = PipelineOptionsValidator.validate(NemoPipelineOptions.class, options);
    return new NemoRunner(nemoOptions);
  }

  /**
   * Method to run the Pipeline.
   * @param pipeline the Pipeline to run.
   * @return The result of the pipeline.
   */
  public NemoPipelineResult run(final Pipeline pipeline) {
    final PipelineVisitor pipelineVisitor = new PipelineVisitor(pipeline, nemoPipelineOptions);
    pipeline.traverseTopologically(pipelineVisitor);
    final NemoPipelineResult nemoPipelineResult = new NemoPipelineResult();
    JobLauncher.launchDAG(pipelineVisitor.getConvertedPipeline(), nemoPipelineOptions.getJobName());
    return nemoPipelineResult;
  }
}
