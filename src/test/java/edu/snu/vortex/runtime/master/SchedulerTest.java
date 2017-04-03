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
import edu.snu.vortex.runtime.common.plan.physical.PhysicalPlanBuilder;
import edu.snu.vortex.runtime.common.plan.physical.TaskGroup;
import edu.snu.vortex.runtime.master.scheduler.Scheduler;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import static org.mockito.Mockito.*;

/**
 * Tests {@link edu.snu.vortex.runtime.master.scheduler.Scheduler}
 */
@RunWith(PowerMockRunner.class)
@PrepareForTest(TaskGroup.class)
public final class SchedulerTest {
  private final Scheduler scheduler = new Scheduler(RuntimeAttribute.SamplePolicy);

  @Test
  public void setUp() {
    final PhysicalPlanBuilder builder = new PhysicalPlanBuilder("TestPlan");

    final TaskGroup taskGroup1 = mock(TaskGroup.class);
    when(taskGroup1.getResourceType()).thenReturn(RuntimeAttribute.Compute);

    final TaskGroup taskGroup2 = mock(TaskGroup.class);
    when(taskGroup1.getResourceType()).thenReturn(RuntimeAttribute.Storage);

    final TaskGroup taskGroup3 = mock(TaskGroup.class);
    when(taskGroup1.getResourceType()).thenReturn(RuntimeAttribute.DistributedStorage);

    builder.createNewStage(3);
    builder.addTaskGroupToCurrentStage(taskGroup1);
    builder.addTaskGroupToCurrentStage(taskGroup1);
    builder.addTaskGroupToCurrentStage(taskGroup1);

    builder.createNewStage(2);
    builder.addTaskGroupToCurrentStage(taskGroup2);
    builder.addTaskGroupToCurrentStage(taskGroup2);

    builder.createNewStage(2);
    builder.addTaskGroupToCurrentStage(taskGroup3);
    builder.addTaskGroupToCurrentStage(taskGroup3);

    scheduler.scheduleJob(builder.build());
  }
}
