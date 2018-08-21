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
package edu.snu.nemo.examples.beam.policy;

import edu.snu.nemo.common.dag.DAG;
import edu.snu.nemo.common.eventhandler.PubSubEventHandlerWrapper;
import edu.snu.nemo.common.ir.edge.IREdge;
import edu.snu.nemo.common.ir.vertex.IRVertex;
import edu.snu.nemo.compiler.optimizer.pass.compiletime.CompileTimePass;
import edu.snu.nemo.compiler.optimizer.pass.compiletime.annotating.UpfrontCloningPass;
import edu.snu.nemo.compiler.optimizer.policy.DefaultPolicy;
import edu.snu.nemo.compiler.optimizer.policy.Policy;
import edu.snu.nemo.compiler.optimizer.policy.PolicyImpl;
import org.apache.reef.tang.Injector;
import java.util.List;

/**
 * A default policy with cloning for tests.
 */
public final class ClonedSchedulingPolicyParallelismFive implements Policy {
  private final Policy policy;
  public ClonedSchedulingPolicyParallelismFive() {
    final List<CompileTimePass> overwritingPasses = DefaultPolicy.BUILDER.getCompileTimePasses();
    overwritingPasses.add(new UpfrontCloningPass()); // CLONING!
    this.policy = new PolicyImpl(
        PolicyTestUtil.overwriteParallelism(5, overwritingPasses),
        DefaultPolicy.BUILDER.getRuntimePasses());
  }
  @Override
  public DAG<IRVertex, IREdge> runCompileTimeOptimization(final DAG<IRVertex, IREdge> dag, final String dagDirectory) {
    return this.policy.runCompileTimeOptimization(dag, dagDirectory);
  }
  @Override
  public void registerRunTimeOptimizations(final Injector injector, final PubSubEventHandlerWrapper pubSubWrapper) {
    this.policy.registerRunTimeOptimizations(injector, pubSubWrapper);
  }
}
