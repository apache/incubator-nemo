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
package edu.snu.coral.compiler.frontend.beam;

import edu.snu.coral.client.JobLauncher;
import edu.snu.coral.common.dag.DAG;
import edu.snu.coral.common.dag.DAGBuilder;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineRunner;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsValidator;

/**
 * Runner class for BEAM programs.
 */
public final class CoralPipelineRunner extends PipelineRunner<CoralPipelineResult> {
  private final CoralPipelineOptions coralPipelineOptions;

  /**
   * BEAM Pipeline Runner.
   * @param coralPipelineOptions PipelineOptions.
   */
  private CoralPipelineRunner(final CoralPipelineOptions coralPipelineOptions) {
    this.coralPipelineOptions = coralPipelineOptions;
  }

  /**
   * Static initializer for creating PipelineRunner with the given options.
   * @param options given PipelineOptions.
   * @return The created PipelineRunner.
   */
  public static PipelineRunner<CoralPipelineResult> fromOptions(final PipelineOptions options) {
    final CoralPipelineOptions coralOptions = PipelineOptionsValidator.validate(CoralPipelineOptions.class, options);
    return new CoralPipelineRunner(coralOptions);
  }

  /**
   * Method to run the Pipeline.
   * @param pipeline the Pipeline to run.
   * @return The result of the pipeline.
   */
  public CoralPipelineResult run(final Pipeline pipeline) {
    final DAGBuilder builder = new DAGBuilder<>();
    final CoralPipelineVisitor coralPipelineVisitor = new CoralPipelineVisitor(builder, coralPipelineOptions);
    pipeline.traverseTopologically(coralPipelineVisitor);
    final DAG dag = builder.build();
    final CoralPipelineResult coralPipelineResult = new CoralPipelineResult();
    JobLauncher.launchDAG(dag);
    return coralPipelineResult;
  }
}
