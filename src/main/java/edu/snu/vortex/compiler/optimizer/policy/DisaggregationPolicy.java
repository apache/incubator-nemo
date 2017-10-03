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

import edu.snu.vortex.compiler.optimizer.pass.compiletime.*;
import edu.snu.vortex.compiler.optimizer.pass.compiletime.annotating.*;
import edu.snu.vortex.compiler.optimizer.pass.compiletime.composite.DisaggregationPass;
import edu.snu.vortex.compiler.optimizer.pass.compiletime.composite.LoopOptimizationPass;

import java.util.Arrays;
import java.util.List;

/**
 * A policy to demonstrate the disaggregation optimization, that performs the job in a Sailfish style.
 */
public final class DisaggregationPolicy implements Policy {
  @Override
  public List<CompileTimePass> getCompileTimePasses() {
    return  Arrays.asList(
        new ParallelismPass(), // Provides parallelism information.
        new LoopOptimizationPass(),
        new DisaggregationPass(),
        new DefaultStagePartitioningPass(),
        new ScheduleGroupPass()
    );
  }
}
