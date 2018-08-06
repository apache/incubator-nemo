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

import edu.snu.nemo.compiler.optimizer.pass.compiletime.*;
import edu.snu.nemo.compiler.optimizer.pass.compiletime.annotating.*;
import edu.snu.nemo.compiler.optimizer.pass.compiletime.composite.DefaultCompositePass;
import edu.snu.nemo.compiler.optimizer.pass.compiletime.composite.LoopOptimizationCompositePass;
import edu.snu.nemo.runtime.common.optimizer.pass.runtime.RuntimePass;

import java.util.List;

/**
 * A policy to demonstrate the disaggregation optimization, that uses GlusterFS as file  storage.
 */
public final class DisaggregationPolicy implements Policy {
  private final Policy policy;

  /**
   * Default constructor.
   */
  public DisaggregationPolicy() {
    this.policy = new PolicyBuilder(false)
        .registerCompileTimePass(new LoopOptimizationCompositePass())
        .registerCompileTimePass(new DefaultCompositePass())
        .registerCompileTimePass(new DisaggregationEdgeDataStorePass())
        .build();
  }

  @Override
  public List<CompileTimePass> getCompileTimePasses() {
    return this.policy.getCompileTimePasses();
  }

  @Override
  public List<RuntimePass<?>> getRuntimePasses() {
    return this.policy.getRuntimePasses();
  }
}
