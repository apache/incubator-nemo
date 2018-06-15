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
package edu.snu.nemo.compiler.frontend.beam;

import edu.snu.nemo.client.JobLauncher;
import edu.snu.nemo.common.dag.DAG;
import edu.snu.nemo.common.dag.DAGBuilder;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineRunner;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsValidator;

/**
 * Runner class for BEAM programs.
 */
public final class NemoPipelineRunner extends PipelineRunner<NemoPipelineResult> {
  private final NemoPipelineOptions nemoPipelineOptions;

  /**
   * BEAM Pipeline Runner.
   * @param nemoPipelineOptions PipelineOptions.
   */
  private NemoPipelineRunner(final NemoPipelineOptions nemoPipelineOptions) {
    this.nemoPipelineOptions = nemoPipelineOptions;
  }

  /**
   * Static initializer for creating PipelineRunner with the given options.
   * @param options given PipelineOptions.
   * @return The created PipelineRunner.
   */
  public static PipelineRunner<NemoPipelineResult> fromOptions(final PipelineOptions options) {
    final NemoPipelineOptions nemoOptions = PipelineOptionsValidator.validate(NemoPipelineOptions.class, options);
    return new NemoPipelineRunner(nemoOptions);
  }

  /**
   * Method to run the Pipeline.
   * @param pipeline the Pipeline to run.
   * @return The result of the pipeline.
   */
  public NemoPipelineResult run(final Pipeline pipeline) {
    final DAGBuilder builder = new DAGBuilder<>();
    final NemoPipelineVisitor nemoPipelineVisitor = new NemoPipelineVisitor(builder, nemoPipelineOptions);
    pipeline.traverseTopologically(nemoPipelineVisitor);
    final DAG dag = builder.build();
    final NemoPipelineResult nemoPipelineResult = new NemoPipelineResult();
    JobLauncher.launchDAG(dag);
    return nemoPipelineResult;
  }
}
