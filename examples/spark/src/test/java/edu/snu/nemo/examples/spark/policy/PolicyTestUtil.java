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
package edu.snu.nemo.examples.spark.policy;

import edu.snu.nemo.compiler.optimizer.pass.compiletime.CompileTimePass;
import edu.snu.nemo.compiler.optimizer.pass.compiletime.annotating.DefaultParallelismPass;

import java.util.List;

/**
 * Test util for testing policies.
 */
public final class PolicyTestUtil {
  /**
   * Overwrite the parallelism of existing policy.
   *
   * @param desiredSourceParallelism     the desired source parallelism to set.
   * @param compileTimePassesToOverwrite the list of compile time passes to overwrite.
   * @return the overwritten policy.
   */
  public static List<CompileTimePass> overwriteParallelism(final int desiredSourceParallelism,
                                                           final List<CompileTimePass> compileTimePassesToOverwrite) {
    final int parallelismPassIdx = compileTimePassesToOverwrite.indexOf(new DefaultParallelismPass());
    compileTimePassesToOverwrite.set(parallelismPassIdx,
        new DefaultParallelismPass(desiredSourceParallelism, 2));
    return compileTimePassesToOverwrite;
  }
}
