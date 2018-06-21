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

import edu.snu.nemo.compiler.optimizer.pass.compiletime.annotating.DefaultParallelismPass;
import edu.snu.nemo.compiler.optimizer.pass.compiletime.annotating.DefaultInterTaskDataStorePass;
import edu.snu.nemo.compiler.optimizer.pass.compiletime.annotating.DefaultScheduleGroupPass;
import edu.snu.nemo.compiler.optimizer.pass.compiletime.CompileTimePass;
import edu.snu.nemo.compiler.optimizer.pass.compiletime.composite.CompositePass;
import edu.snu.nemo.runtime.common.optimizer.pass.runtime.RuntimePass;

import java.util.Arrays;
import java.util.List;

/**
 * A simple example policy to demonstrate a policy with a separate, refactored pass.
 * It simply performs what is done with the default pass.
 * This example simply shows that users can define their own pass in their policy.
 */
public final class DefaultPolicyWithSeparatePass implements Policy {
  private final Policy policy;

  /**
   * Default constructor.
   */
  public DefaultPolicyWithSeparatePass() {
    this.policy = new PolicyBuilder(true)
        .registerCompileTimePass(new DefaultParallelismPass())
        .registerCompileTimePass(new RefactoredPass())
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

  /**
   * A simple custom pass consisted of the two passes at the end of the default pass.
   */
  public final class RefactoredPass extends CompositePass {
    /**
     * Default constructor.
     */
    RefactoredPass() {
      super(Arrays.asList(
          new DefaultInterTaskDataStorePass(),
          new DefaultScheduleGroupPass()
      ));
    }
  }
}
