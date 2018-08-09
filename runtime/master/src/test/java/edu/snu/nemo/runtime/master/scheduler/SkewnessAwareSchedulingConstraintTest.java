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

import edu.snu.nemo.runtime.common.RuntimeIdGenerator;
import edu.snu.nemo.runtime.common.data.HashRange;
import edu.snu.nemo.runtime.common.data.KeyRange;
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
@PrepareForTest({ExecutorRepresenter.class, Task.class, HashRange.class, StageEdge.class})
public final class SkewnessAwareSchedulingConstraintTest {

  private static StageEdge mockStageEdge() {
    final Map<Integer, KeyRange> taskIdxToKeyRange = new HashMap<>();

    final HashRange skewedHashRange1 = mock(HashRange.class);
    when(skewedHashRange1.isSkewed()).thenReturn(true);
    final HashRange skewedHashRange2 = mock(HashRange.class);
    when(skewedHashRange2.isSkewed()).thenReturn(true);
    final HashRange hashRange = mock(HashRange.class);
    when(hashRange.isSkewed()).thenReturn(false);

    taskIdxToKeyRange.put(0, skewedHashRange1);
    taskIdxToKeyRange.put(1, skewedHashRange2);
    taskIdxToKeyRange.put(2, hashRange);

    final StageEdge inEdge = mock(StageEdge.class);
    when(inEdge.getTaskIdxToKeyRange()).thenReturn(taskIdxToKeyRange);

    return inEdge;
  }

  private static Task mockTask(final int taskIdx, final List<StageEdge> inEdges) {
    final Task task = mock(Task.class);
    when(task.getTaskId()).thenReturn(RuntimeIdGenerator.generateTaskId("Stage-0", taskIdx, 0));
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
    final StageEdge inEdge = mockStageEdge();
    final Task task0 = mockTask(0, Arrays.asList(inEdge));
    final Task task1 = mockTask(1, Arrays.asList(inEdge));
    final Task task2 = mockTask(2, Arrays.asList(inEdge));
    final ExecutorRepresenter e0 = mockExecutorRepresenter(task0);

    assertEquals(true, schedulingConstraint.testSchedulability(e0, task2));
    assertEquals(false, schedulingConstraint.testSchedulability(e0, task1));
  }
}
