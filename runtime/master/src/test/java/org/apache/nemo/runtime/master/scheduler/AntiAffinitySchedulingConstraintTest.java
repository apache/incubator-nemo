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

import org.apache.nemo.common.ir.executionproperty.ExecutionPropertyMap;
import org.apache.nemo.common.ir.vertex.IRVertex;
import org.apache.nemo.common.ir.vertex.executionproperty.ResourceAntiAffinityProperty;
import org.apache.nemo.runtime.common.RuntimeIdManager;
import org.apache.nemo.common.ir.edge.Stage;
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
 * Test cases for {@link AntiAffinitySchedulingConstraint}.
 */
@RunWith(PowerMockRunner.class)
@PrepareForTest({ExecutorRepresenter.class, Task.class, Stage.class, IRVertex.class})
public final class AntiAffinitySchedulingConstraintTest {
  private final static int FIRST_ATTEMPT = 0;
  private final static String EXEc_MAP_ID = "ID";

  private static Task mockTask(final int taskIdx, final ExecutionPropertyMap executionPropertyMap) {
    final Task task = mock(Task.class);
    when(task.getTaskId()).thenReturn(RuntimeIdManager.generateTaskId("Stage0", taskIdx, FIRST_ATTEMPT));
    when(task.getExecutionProperties()).thenReturn(executionPropertyMap);
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
   * {@link AntiAffinitySchedulingConstraint} should schedule tasks to different executors.
   */
  @Test
  public void testScheduleSkewedTasks() {
    final SchedulingConstraint schedulingConstraint = new AntiAffinitySchedulingConstraint();

    // Create a StageEdge where two out of three are skewed hash ranges.
    final HashSet<Integer> antiAffinityGroup = new HashSet<>(Arrays.asList(0, 1));
    final ExecutionPropertyMap emap = new ExecutionPropertyMap(EXEc_MAP_ID);
    emap.put(ResourceAntiAffinityProperty.of(antiAffinityGroup));


    final Task task0 = mockTask(0, emap);  // anti-affinity task
    final Task task1 = mockTask(1, emap);  // anti-affinity task
    final Task task2 = mockTask(2, emap);  // normal task
    final ExecutorRepresenter e0 = mockExecutorRepresenter(task0);  // schedule task0 to e0

    assertEquals(false, schedulingConstraint.testSchedulability(e0, task1));
    assertEquals(true, schedulingConstraint.testSchedulability(e0, task2));
  }
}
