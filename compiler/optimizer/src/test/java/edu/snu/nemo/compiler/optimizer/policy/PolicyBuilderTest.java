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

import edu.snu.nemo.common.exception.CompileTimeOptimizationException;
import edu.snu.nemo.compiler.optimizer.pass.compiletime.annotating.DefaultScheduleGroupPass;
import edu.snu.nemo.compiler.optimizer.pass.compiletime.composite.TransientResourceCompositePass;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public final class PolicyBuilderTest {
  @Test
  public void testDisaggregationPolicy() {
    assertEquals(17, DisaggregationPolicy.BUILDER.getCompileTimePasses().size());
    assertEquals(0, DisaggregationPolicy.BUILDER.getRuntimePasses().size());
  }

  @Test
  public void testTransientResourcePolicy() {
    assertEquals(19, TransientResourcePolicy.BUILDER.getCompileTimePasses().size());
    assertEquals(0, TransientResourcePolicy.BUILDER.getRuntimePasses().size());
  }

  @Test
  public void testDataSkewPolicy() {
    assertEquals(21, DataSkewPolicy.BUILDER.getCompileTimePasses().size());
    assertEquals(1, DataSkewPolicy.BUILDER.getRuntimePasses().size());
  }

  @Test
  public void testShouldFailPolicy() {
    try {
      final Policy failPolicy = new PolicyBuilder()
          .registerCompileTimePass(new TransientResourceCompositePass())
          .registerCompileTimePass(new DefaultScheduleGroupPass())
          .build();
    } catch (Exception e) { // throw an exception if default execution properties are not set.
      assertTrue(e instanceof CompileTimeOptimizationException);
      assertTrue(e.getMessage().contains("Prerequisite ExecutionProperty hasn't been met"));
    }
  }
}
