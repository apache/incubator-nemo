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

import edu.snu.nemo.compiler.optimizer.pass.compiletime.CompileTimePass;
import edu.snu.nemo.compiler.optimizer.pass.compiletime.annotating.DefaultParallelismPass;
import edu.snu.nemo.compiler.optimizer.policy.Policy;
import edu.snu.nemo.runtime.common.optimizer.pass.runtime.RuntimePass;

import java.util.List;

/**
 * Test util for testing policies.
 */
public final class PolicyTestUtil {
  /**
   * Overwrite the parallelism of existing policy.
   *
   * @param desiredSourceParallelism       the desired source parallelism to set.
   * @param policyToOverwriteCanonicalName the name of the policy to overwrite parallelism.
   * @return the overwritten policy.
   */
  public static Policy overwriteParallelism(final int desiredSourceParallelism,
                                            final String policyToOverwriteCanonicalName) {
    final Policy policyToOverwrite;
    try {
      policyToOverwrite = (Policy) Class.forName(policyToOverwriteCanonicalName).newInstance();
    } catch (final ClassNotFoundException | InstantiationException | IllegalAccessException e) {
      throw new RuntimeException(e);
    }
    final List<CompileTimePass> compileTimePasses = policyToOverwrite.getCompileTimePasses();
    final int parallelismPassIdx = compileTimePasses.indexOf(new DefaultParallelismPass());
    compileTimePasses.set(parallelismPassIdx, new DefaultParallelismPass(desiredSourceParallelism, 2));
    final List<RuntimePass<?>> runtimePasses = policyToOverwrite.getRuntimePasses();

    return new Policy() {
      @Override
      public List<CompileTimePass> getCompileTimePasses() {
        return compileTimePasses;
      }

      @Override
      public List<RuntimePass<?>> getRuntimePasses() {
        return runtimePasses;
      }
    };
  }
}
