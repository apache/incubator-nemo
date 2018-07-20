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
package edu.snu.nemo.runtime.master.scheduler;

import edu.snu.nemo.common.DataSkewMetricFactory;
import edu.snu.nemo.common.ir.edge.IREdge;
import edu.snu.nemo.common.ir.edge.executionproperty.DataCommunicationPatternProperty;
import edu.snu.nemo.common.ir.edge.executionproperty.DataFlowModelProperty;
import edu.snu.nemo.common.ir.edge.executionproperty.DataSkewMetricProperty;
import edu.snu.nemo.common.ir.vertex.IRVertex;
import edu.snu.nemo.runtime.common.RuntimeIdGenerator;
import edu.snu.nemo.common.HashRange;
import edu.snu.nemo.common.KeyRange;
import edu.snu.nemo.runtime.common.plan.Stage;
import edu.snu.nemo.runtime.common.plan.StageEdge;
import edu.snu.nemo.runtime.common.plan.Task;
import edu.snu.nemo.runtime.master.resource.ExecutorRepresenter;
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
@PrepareForTest({ExecutorRepresenter.class, Task.class, Stage.class, HashRange.class,
IRVertex.class, IREdge.class})
public final class SkewnessAwareSchedulingConstraintTest {

  private static StageEdge mockStageEdge(final int numSkewedHashRange,
                                         final int numTotalHashRange) {
    final Map<Integer, KeyRange> taskIdxToKeyRange = new HashMap<>();

    for (int taskIdx = 0; taskIdx < numTotalHashRange; taskIdx++) {
      final HashRange hashRange = mock(HashRange.class);
      if (taskIdx < numSkewedHashRange) {
        when(hashRange.isSkewed()).thenReturn(true);
      } else {
        when(hashRange.isSkewed()).thenReturn(false);
      }
      taskIdxToKeyRange.put(taskIdx, hashRange);
    }

    final IRVertex srcMockVertex = mock(IRVertex.class);
    final IRVertex dstMockVertex = mock(IRVertex.class);
    final Stage srcMockStage = mock(Stage.class);
    final Stage dstMockStage = mock(Stage.class);

    final IREdge dummyIREdge = new IREdge(DataCommunicationPatternProperty.Value.Shuffle, srcMockVertex, dstMockVertex);
    dummyIREdge.setProperty(DataFlowModelProperty.of(DataFlowModelProperty.Value.Pull));
    dummyIREdge.setProperty(DataSkewMetricProperty.of(new DataSkewMetricFactory(taskIdxToKeyRange)));
    final StageEdge dummyEdge = new StageEdge("Edge-0", dummyIREdge.getExecutionProperties(),
        srcMockVertex, dstMockVertex, srcMockStage, dstMockStage, false);

    return dummyEdge;
  }

  private static Task mockTask(final int taskIdx, final List<StageEdge> inEdges) {
    final Task task = mock(Task.class);
    when(task.getTaskId()).thenReturn(RuntimeIdGenerator.generateTaskId(taskIdx, "Stage-0"));
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
