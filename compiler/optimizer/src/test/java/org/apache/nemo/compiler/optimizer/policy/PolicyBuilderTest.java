/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.nemo.compiler.optimizer.policy;

import org.apache.nemo.common.exception.CompileTimeOptimizationException;
import org.apache.nemo.compiler.optimizer.pass.compiletime.annotating.DefaultScheduleGroupPass;
import org.apache.nemo.compiler.optimizer.pass.compiletime.composite.TransientResourceCompositePass;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public final class PolicyBuilderTest {
  @Test
  public void testDisaggregationPolicy() {
    assertEquals(15, DisaggregationPolicy.BUILDER.getCompileTimePasses().size());
    assertEquals(0, DisaggregationPolicy.BUILDER.getRunTimePasses().size());
  }

  @Test
  public void testTransientResourcePolicy() {
    assertEquals(16, TransientResourcePolicy.BUILDER.getCompileTimePasses().size());
    assertEquals(0, TransientResourcePolicy.BUILDER.getRunTimePasses().size());
  }

  @Test
  public void testDataSkewPolicy() {
    assertEquals(17, DataSkewPolicy.BUILDER.getCompileTimePasses().size());
    assertEquals(1, DataSkewPolicy.BUILDER.getRunTimePasses().size());
  }

  @Test
  public void testIntermediateAccumulatorPolicy() {
    assertEquals(11, IntermediateAccumulatorPolicy.BUILDER.getCompileTimePasses().size());
    assertEquals(0, IntermediateAccumulatorPolicy.BUILDER.getRunTimePasses().size());
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
