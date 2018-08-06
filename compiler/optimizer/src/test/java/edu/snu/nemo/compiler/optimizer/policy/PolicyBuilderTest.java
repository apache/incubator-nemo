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
import edu.snu.nemo.compiler.optimizer.pass.compiletime.composite.PadoCompositePass;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public final class PolicyBuilderTest {
  @Test
  public void testDisaggregationPolicy() {
    final Policy disaggregationPolicy = new DisaggregationPolicy();
    assertEquals(17, disaggregationPolicy.getCompileTimePasses().size());
    assertEquals(0, disaggregationPolicy.getRuntimePasses().size());
  }

  @Test
  public void testPadoPolicy() {
    final Policy padoPolicy = new PadoPolicy();
    assertEquals(19, padoPolicy.getCompileTimePasses().size());
    assertEquals(0, padoPolicy.getRuntimePasses().size());
  }

  @Test
  public void testDataSkewPolicy() {
    final Policy dataSkewPolicy = new DataSkewPolicy();
    assertEquals(21, dataSkewPolicy.getCompileTimePasses().size());
    assertEquals(1, dataSkewPolicy.getRuntimePasses().size());
  }

  @Test
  public void testShouldFailPolicy() {
    try {
      final Policy failPolicy = new PolicyBuilder()
          .registerCompileTimePass(new PadoCompositePass())
          .registerCompileTimePass(new DefaultScheduleGroupPass())
          .build();
    } catch (Exception e) { // throw an exception if default execution properties are not set.
      assertTrue(e instanceof CompileTimeOptimizationException);
      assertTrue(e.getMessage().contains("Prerequisite ExecutionProperty hasn't been met"));
    }
  }
}
