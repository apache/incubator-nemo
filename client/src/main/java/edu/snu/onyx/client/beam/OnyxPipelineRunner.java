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
package edu.snu.onyx.client.beam;

import edu.snu.onyx.client.JobLauncher;
import edu.snu.onyx.common.dag.DAG;
import edu.snu.onyx.common.dag.DAGBuilder;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineRunner;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsValidator;

/**
 * Runner class for BEAM programs.
 */
public final class OnyxPipelineRunner extends PipelineRunner<OnyxPipelineResult> {
  private final OnyxPipelineOptions onyxPipelineOptions;

  /**
   * BEAM Pipeline Runner.
   * @param onyxPipelineOptions PipelineOptions.
   */
  private OnyxPipelineRunner(final OnyxPipelineOptions onyxPipelineOptions) {
    this.onyxPipelineOptions = onyxPipelineOptions;
  }

  /**
   * Static initializer for creating PipelineRunner with the given options.
   * @param options given PipelineOptions.
   * @return The created PipelineRunner.
   */
  public static PipelineRunner<OnyxPipelineResult> fromOptions(final PipelineOptions options) {
    final OnyxPipelineOptions onyxOptions = PipelineOptionsValidator.validate(OnyxPipelineOptions.class, options);
    return new OnyxPipelineRunner(onyxOptions);
  }

  /**
   * Method to run the Pipeline.
   * @param pipeline the Pipeline to run.
   * @return The result of the pipeline.
   */
  public OnyxPipelineResult run(final Pipeline pipeline) {
    final DAGBuilder builder = new DAGBuilder<>();
    final OnyxPipelineVisitor onyxPipelineVisitor = new OnyxPipelineVisitor(builder, onyxPipelineOptions);
    pipeline.traverseTopologically(onyxPipelineVisitor);
    final DAG dag = builder.build();
    final OnyxPipelineResult onyxPipelineResult = new OnyxPipelineResult();
    JobLauncher.launchDAG(dag);
    return onyxPipelineResult;
  }
}
