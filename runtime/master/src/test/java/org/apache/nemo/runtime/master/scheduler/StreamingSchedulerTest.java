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

import org.apache.nemo.common.eventhandler.PubSubEventHandlerWrapper;
import org.apache.nemo.runtime.common.RuntimeIdManager;
import org.apache.nemo.runtime.common.plan.PhysicalPlan;
import org.apache.nemo.runtime.common.plan.TestPlanGenerator;
import org.apache.nemo.runtime.master.BlockManagerMaster;
import org.apache.nemo.runtime.master.PipeManagerMaster;
import org.apache.nemo.runtime.master.PlanStateManager;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.Mockito.*;

/**
 * Tests {@link StreamingScheduler}.
 */
@RunWith(PowerMockRunner.class)
@PrepareForTest({BlockManagerMaster.class, PubSubEventHandlerWrapper.class,
  TaskDispatcher.class, PendingTaskCollectionPointer.class, ExecutorRegistry.class, PlanStateManager.class,
  PipeManagerMaster.class})
public final class StreamingSchedulerTest {
  private static final int ATTEMPTS_PER_STAGE = 2;

  private StreamingScheduler scheduler;
  private PendingTaskCollectionPointer pendingTaskCollectionPointer;

  @Before
  public void setUp() throws Exception {
    final TaskDispatcher taskDispatcher = mock(TaskDispatcher.class);
    final PendingTaskCollectionPointer pendingTaskCollectionPointer = mock(PendingTaskCollectionPointer.class);
    this.pendingTaskCollectionPointer = pendingTaskCollectionPointer;
    final ExecutorRegistry executorRegistry = mock(ExecutorRegistry.class);
    final PlanStateManager planStateManager = mock(PlanStateManager.class);
    final PipeManagerMaster pipeManagerMaster = mock(PipeManagerMaster.class);

    when(planStateManager.getTaskAttemptsToSchedule(any())).thenAnswer(invocationOnMock -> {
      final String stageId = invocationOnMock.getArgument(0);
      return generateAttempts(stageId);
    });

    scheduler = new StreamingScheduler(
      taskDispatcher, pendingTaskCollectionPointer, executorRegistry, planStateManager, pipeManagerMaster);
  }

  private List<String> generateAttempts(final String stageId) {
    return IntStream.range(0, ATTEMPTS_PER_STAGE)
      .mapToObj(taskIndex -> RuntimeIdManager.generateTaskId(stageId, taskIndex, 0))
      .collect(Collectors.toList());
  }

  @Test(timeout = 10000)
  public void testScheduleEverything() throws Exception {
    final PhysicalPlan physicalPlan =
      TestPlanGenerator.generatePhysicalPlan(TestPlanGenerator.PlanType.TwoVerticesJoined, false);
    final int numOfTotalTasks = physicalPlan.getStageDAG().getVertices().size() * ATTEMPTS_PER_STAGE;
    scheduler.schedulePlan(physicalPlan, 1);
    verify(pendingTaskCollectionPointer).setToOverwrite(argThat(tasks -> tasks.size() == numOfTotalTasks));
  }
}
