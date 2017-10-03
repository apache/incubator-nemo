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
package edu.snu.vortex.compiler.optimizer.pass.compiletime.annotating;

import edu.snu.vortex.client.JobLauncher;
import edu.snu.vortex.common.dag.DAG;
import edu.snu.vortex.compiler.CompilerTestUtil;
import edu.snu.vortex.compiler.ir.IREdge;
import edu.snu.vortex.compiler.ir.IRVertex;
import edu.snu.vortex.compiler.ir.executionproperty.ExecutionProperty;
import edu.snu.vortex.compiler.optimizer.Optimizer;
import edu.snu.vortex.compiler.optimizer.TestPolicy;
import edu.snu.vortex.compiler.optimizer.pass.compiletime.annotating.ScheduleGroupPass;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Test {@link ScheduleGroupPass}.
 */
@RunWith(PowerMockRunner.class)
@PrepareForTest(JobLauncher.class)
public final class ScheduleGroupPassTest {
  @Before
  public void setUp() throws Exception {
  }

  @Test
  public void testAnnotatingPass() {
    final AnnotatingPass scheduleGroupPass = new ScheduleGroupPass();
    assertEquals(ExecutionProperty.Key.ScheduleGroupIndex, scheduleGroupPass.getExecutionPropertyToModify());
  }

  @Test
  /**
   * This test ensures that a topologically sorted DAG has an increasing sequence of schedule group indexes.
   */
  public void testScheduleGroupPass() throws Exception {
    final DAG<IRVertex, IREdge> compiledDAG = CompilerTestUtil.compileALSDAG();
    final DAG<IRVertex, IREdge> processedDAG = Optimizer.optimize(compiledDAG, new TestPolicy(), "");

    Integer previousScheduleGroupIndex = 0;
    for (final IRVertex irVertex : processedDAG.getTopologicalSort()) {
      assertTrue(irVertex.getProperty(ExecutionProperty.Key.ScheduleGroupIndex) != null);
      final Integer currentScheduleGroupIndex = irVertex.getProperty(ExecutionProperty.Key.ScheduleGroupIndex);
      assertTrue(currentScheduleGroupIndex >= previousScheduleGroupIndex);
      if (currentScheduleGroupIndex > previousScheduleGroupIndex) {
        previousScheduleGroupIndex = currentScheduleGroupIndex;
      }
    }
  }
}
