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
package edu.snu.vortex.compiler.optimizer.policy;

import edu.snu.vortex.common.dag.DAG;
import edu.snu.vortex.compiler.ir.IREdge;
import edu.snu.vortex.compiler.ir.IRVertex;
import edu.snu.vortex.compiler.optimizer.pass.DefaultStagePartitioningPass;
import edu.snu.vortex.compiler.optimizer.pass.ParallelismPass;
import edu.snu.vortex.compiler.optimizer.pass.ScheduleGroupPass;
import edu.snu.vortex.compiler.optimizer.pass.StaticOptimizationPass;

import java.util.Arrays;
import java.util.List;

/**
 * A simple example policy to demonstrate a policy with a separate, refactored pass.
 * It simply performs what is done with the default pass.
 * This example simply shows that users can define their own pass in their policy.
 */
public final class DefaultPolicyWithSeparatePass implements Policy {
  @Override
  public List<StaticOptimizationPass> getOptimizationPasses() {
    return Arrays.asList(
        new ParallelismPass(),
        new RefactoredPass()
    );
  }

  /**
   * A simple custom pass consisted of the two passes at the end of the default pass.
   */
  public final class RefactoredPass implements StaticOptimizationPass {
    @Override
    public DAG<IRVertex, IREdge> apply(final DAG<IRVertex, IREdge> dag) {
      return new ScheduleGroupPass().apply(new DefaultStagePartitioningPass().apply(dag));
    }
  }
}
