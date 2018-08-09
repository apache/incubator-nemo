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
package edu.snu.nemo.compiler.optimizer.policy;

import edu.snu.nemo.common.dag.DAG;
import edu.snu.nemo.common.eventhandler.PubSubEventHandlerWrapper;
import edu.snu.nemo.common.ir.edge.IREdge;
import edu.snu.nemo.common.ir.vertex.IRVertex;
import edu.snu.nemo.compiler.optimizer.pass.compiletime.annotating.DefaultParallelismPass;
import edu.snu.nemo.compiler.optimizer.pass.compiletime.annotating.DefaultDataStorePass;
import edu.snu.nemo.compiler.optimizer.pass.compiletime.annotating.DefaultScheduleGroupPass;
import edu.snu.nemo.compiler.optimizer.pass.compiletime.composite.CompositePass;
import org.apache.reef.tang.Injector;

import java.util.Arrays;

/**
 * A simple example policy to demonstrate a policy with a separate, refactored pass.
 * It simply performs what is done with the default pass.
 * This example simply shows that users can define their own pass in their policy.
 */
public final class DefaultPolicyWithSeparatePass implements Policy {
  public static final PolicyBuilder BUILDER =
      new PolicyBuilder(true)
          .registerCompileTimePass(new DefaultParallelismPass())
          .registerCompileTimePass(new RefactoredPass());
  private final Policy policy;

  /**
   * Default constructor.
   */
  public DefaultPolicyWithSeparatePass() {
    this.policy = BUILDER.build();
  }

  @Override
  public DAG<IRVertex, IREdge> runCompileTimeOptimization(final DAG<IRVertex, IREdge> dag, final String dagDirectory)
      throws Exception {
    return this.policy.runCompileTimeOptimization(dag, dagDirectory);
  }

  @Override
  public void registerRunTimeOptimizations(final Injector injector, final PubSubEventHandlerWrapper pubSubWrapper) {
    this.policy.registerRunTimeOptimizations(injector, pubSubWrapper);
  }

  /**
   * A simple custom pass consisted of the two passes at the end of the default pass.
   */
  public static final class RefactoredPass extends CompositePass {
    /**
     * Default constructor.
     */
    RefactoredPass() {
      super(Arrays.asList(
          new DefaultDataStorePass(),
          new DefaultScheduleGroupPass()
      ));
    }
  }
}
