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
package org.apache.nemo.runtime.master.scheduler;

import org.apache.nemo.common.DataSkewMetricFactory;
import org.apache.nemo.common.ir.edge.IREdge;
import org.apache.nemo.common.ir.edge.executionproperty.CommunicationPatternProperty;
import org.apache.nemo.common.ir.edge.executionproperty.DataFlowProperty;
import org.apache.nemo.common.ir.vertex.IRVertex;
import org.apache.nemo.runtime.common.RuntimeIdManager;
import org.apache.nemo.common.KeyRange;
import org.apache.nemo.runtime.common.plan.Stage;
import org.apache.nemo.runtime.common.plan.StageEdge;
import org.apache.nemo.runtime.common.plan.Task;
import org.apache.nemo.runtime.master.resource.ExecutorRepresenter;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.util.*;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Test cases for {@link SkewnessAwareSchedulingConstraint}.
 */
@RunWith(PowerMockRunner.class)
@PrepareForTest({ExecutorRepresenter.class, Task.class, Stage.class, KeyRange.class,
IRVertex.class, IREdge.class})
public final class SkewnessAwareSchedulingConstraintTest {
  private final static int FIRST_ATTEMPT = 0;

  private static StageEdge mockStageEdge(final int numSkewedHashRange,
                                         final int numTotalHashRange) {
    final Map<Integer, KeyRange> taskIdxToKeyRange = new HashMap<>();

    for (int taskIdx = 0; taskIdx < numTotalHashRange; taskIdx++) {
      final KeyRange keyRange = mock(KeyRange.class);
      if (taskIdx < numSkewedHashRange) {
        when(keyRange.isSkewed()).thenReturn(true);
      } else {
        when(keyRange.isSkewed()).thenReturn(false);
      }
      taskIdxToKeyRange.put(taskIdx, keyRange);
    }

    final IRVertex srcMockVertex = mock(IRVertex.class);
    final IRVertex dstMockVertex = mock(IRVertex.class);
    final Stage srcMockStage = mock(Stage.class);
    final Stage dstMockStage = mock(Stage.class);

    final IREdge dummyIREdge = new IREdge(CommunicationPatternProperty.Value.Shuffle, srcMockVertex, dstMockVertex);
    dummyIREdge.setProperty(DataFlowProperty.of(DataFlowProperty.Value.Pull));
    dummyIREdge.setProperty(DataSkewMetricProperty.of(new DataSkewMetricFactory(taskIdxToKeyRange)));
    final StageEdge dummyEdge = new StageEdge("Edge0", dummyIREdge.getExecutionProperties(),
        srcMockVertex, dstMockVertex, srcMockStage, dstMockStage);

    return dummyEdge;
  }

  private static Task mockTask(final int taskIdx, final List<StageEdge> inEdges) {
    final Task task = mock(Task.class);
    when(task.getTaskId()).thenReturn(RuntimeIdManager.generateTaskId("Stage0", taskIdx, FIRST_ATTEMPT));
    when(task.getTaskIncomingEdges()).thenReturn(inEdges);
    return task;
  }

  private static ExecutorRepresenter mockExecutorRepresenter(final Task task) {
    final ExecutorRepresenter executorRepresenter = mock(ExecutorRepresenter.class);
    final Set<Task> runningTasks = new HashSet<>();
    runningTasks.add(task);
    when(executorRepresenter.getRunningTasks()).thenReturn(runningTasks);
    return executorRepresenter;
  }

  /**
   * {@link SkewnessAwareSchedulingConstraint} should schedule Tasks assigned with skewed partitions
   * to different executors.
   */
  @Test
  public void testScheduleSkewedTasks() {
    final SchedulingConstraint schedulingConstraint = new SkewnessAwareSchedulingConstraint();
    // Create a StageEdge where two out of three are skewed hash ranges.
    final StageEdge inEdge = mockStageEdge(2, 3);
    final Task task0 = mockTask(0, Arrays.asList(inEdge));  // skewed task
    final Task task1 = mockTask(1, Arrays.asList(inEdge));  // skewed task
    final Task task2 = mockTask(2, Arrays.asList(inEdge));  // non-skewed task
    final ExecutorRepresenter e0 = mockExecutorRepresenter(task0);  // schedule skewed task to e0

    assertEquals(true, schedulingConstraint.testSchedulability(e0, task2));
    assertEquals(false, schedulingConstraint.testSchedulability(e0, task1));
  }
}
