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
package edu.snu.nemo.tests.compiler.optimizer.pass.compiletime.annotating;

import edu.snu.nemo.client.JobLauncher;
import edu.snu.nemo.common.dag.DAG;
import edu.snu.nemo.common.ir.edge.IREdge;
import edu.snu.nemo.common.ir.vertex.IRVertex;
import edu.snu.nemo.common.ir.vertex.executionproperty.ScheduleGroupIndexProperty;
import edu.snu.nemo.compiler.optimizer.CompiletimeOptimizer;
import edu.snu.nemo.tests.compiler.optimizer.policy.TestPolicy;
import edu.snu.nemo.compiler.optimizer.pass.compiletime.annotating.AnnotatingPass;
import edu.snu.nemo.compiler.optimizer.pass.compiletime.annotating.ScheduleGroupPass;
import edu.snu.nemo.tests.compiler.CompilerTestUtil;
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
    assertEquals(ScheduleGroupIndexProperty.class, scheduleGroupPass.getExecutionPropertyToModify());
  }

  /**
   * This test ensures that a topologically sorted DAG has an increasing sequence of schedule group indexes.
   */
  @Test
  public void testScheduleGroupPass() throws Exception {
    final DAG<IRVertex, IREdge> compiledDAG = CompilerTestUtil.compileALSDAG();
    final DAG<IRVertex, IREdge> processedDAG = CompiletimeOptimizer.optimize(compiledDAG,
        new TestPolicy(), "");

    for (final IRVertex irVertex : processedDAG.getTopologicalSort()) {
      final Integer currentScheduleGroupIndex = irVertex.getPropertyValue(ScheduleGroupIndexProperty.class).get();
      final Integer largestScheduleGroupIndexOfParent = processedDAG.getParents(irVertex.getId()).stream()
          .mapToInt(v -> v.getPropertyValue(ScheduleGroupIndexProperty.class).get())
          .max().orElse(0);
      assertTrue(currentScheduleGroupIndex >= largestScheduleGroupIndexOfParent);
    }
  }
}
