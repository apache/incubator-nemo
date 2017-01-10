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
package edu.snu.vortex.compiler.frontend.beam;

import edu.snu.vortex.compiler.ir.DAG;
import edu.snu.vortex.compiler.ir.DAGBuilder;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.runners.PipelineRunner;

public final class Runner extends PipelineRunner<Result> {

  public static PipelineRunner<Result> fromOptions(PipelineOptions options) {
    return new Runner();
  }

  public Result run(final Pipeline pipeline) {
    final DAGBuilder builder = new DAGBuilder();
    final Visitor visitor = new Visitor(builder);
    pipeline.traverseTopologically(visitor);
    final DAG dag = builder.build();
    BeamFrontend.supplyDAGFromRunner(dag);
    return new Result();
  }
}
