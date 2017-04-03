/*
 * Copyright (C) 2016 Seoul National University
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
package edu.snu.vortex.runtime.master;

import edu.snu.vortex.runtime.common.RuntimeAttribute;
import edu.snu.vortex.runtime.common.plan.physical.TaskGroup;
import edu.snu.vortex.runtime.master.scheduler.SampleScheduler;
import edu.snu.vortex.runtime.master.scheduler.SchedulingPolicy;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.util.Optional;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Tests {@link SampleScheduler}
 */
@RunWith(PowerMockRunner.class)
@PrepareForTest(TaskGroup.class)
public final class SampleSchedulerTest {
  private SchedulingPolicy schedulingPolicy;

  @Before
  public void setUp() {
    schedulingPolicy = SampleScheduler.newInstance();
  }

  @Test
  public void checkScheduleTimeout() {
    assertEquals(schedulingPolicy.getScheduleTimeout(), 2000);
  }

  @Test
  public void testTwoTypesOfExecutors() {
    final ExecutorRepresenter executor1 = new ExecutorRepresenter("Executor-1", RuntimeAttribute.Compute, 1);

    final TaskGroup computeTaskGroup = mock(TaskGroup.class);
    when(computeTaskGroup.getResourceType()).thenReturn(RuntimeAttribute.Compute);

    final ExecutorRepresenter executor2 = new ExecutorRepresenter("Executor-2", RuntimeAttribute.Storage, 1);

    final TaskGroup storageTaskGroup = mock(TaskGroup.class);
    when(storageTaskGroup.getResourceType()).thenReturn(RuntimeAttribute.Storage);

    // Add a compute executor
    schedulingPolicy.onExecutorAdded(executor1);

    // Try scheduling a compute task group to the available compute executor
    Optional<String> executorId = schedulingPolicy.attemptSchedule(computeTaskGroup);
    assertEquals(executor1.getExecutorId(), executorId.get());

    // Mark the compute task group scheduled
    schedulingPolicy.onTaskGroupScheduled(executor1, computeTaskGroup);

    // Try scheduling a compute task group, but this time there is no available compute executor
    executorId = schedulingPolicy.attemptSchedule(computeTaskGroup);
    // Wait for 2 seconds
    assertFalse(executorId.isPresent());

    // Add a storage executor
    schedulingPolicy.onExecutorAdded(executor2);

    // Try scheduling a compute task group, and there is still no available compute executor
    executorId = schedulingPolicy.attemptSchedule(computeTaskGroup);
    // Wait for 2 seconds
    assertFalse(executorId.isPresent());

    // Mark the compute task group complete
    schedulingPolicy.onTaskGroupExecutionComplete(executor1, computeTaskGroup);

    // Try scheduling a compute task group to the available compute executor
    executorId = schedulingPolicy.attemptSchedule(computeTaskGroup);
    assertEquals(executor1.getExecutorId(), executorId.get());

    // Try scheduling a storage task group to the available storage executor
    executorId = schedulingPolicy.attemptSchedule(storageTaskGroup);
    assertEquals(executor2.getExecutorId(), executorId.get());
  }
}
