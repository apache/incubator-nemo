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
package edu.snu.onyx.compiler.optimizer;

import edu.snu.onyx.compiler.optimizer.pass.compiletime.annotating.*;
import edu.snu.onyx.compiler.optimizer.pass.compiletime.CompileTimePass;
import edu.snu.onyx.compiler.optimizer.pass.runtime.RuntimePass;
import edu.snu.onyx.compiler.optimizer.policy.Policy;

import java.util.ArrayList;
import java.util.List;

/**
 * A policy for tests.
 */
public final class TestPolicy implements Policy {
  private final boolean testPushPolicy;

  public TestPolicy() {
    this(false);
  }

  public TestPolicy(final boolean testPushPolicy) {
    this.testPushPolicy = testPushPolicy;
  }

  @Override
  public List<CompileTimePass> getCompileTimePasses() {
    List<CompileTimePass> policy = new ArrayList<>();
    policy.add(new DefaultVertexExecutorPlacementPass());
    policy.add(new DefaultPartitionerPass());
    policy.add(new DefaultEdgeDataFlowModelPass());
    policy.add(new DefaultEdgeDataStorePass());
    policy.add(new DefaultStagePartitioningPass());
    policy.add(new ScheduleGroupPass());

    if (testPushPolicy) {
      policy.add(3, new ScatterGatherEdgePushPass());
    }
    return policy;
  }

  @Override
  public List<RuntimePass<?>> getRuntimePasses() {
    return new ArrayList<>();
  }
}
