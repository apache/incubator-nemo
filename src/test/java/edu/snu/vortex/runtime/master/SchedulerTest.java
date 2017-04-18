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
import edu.snu.vortex.runtime.common.plan.physical.*;
import edu.snu.vortex.runtime.master.scheduler.Scheduler;
import edu.snu.vortex.utils.dag.DAGBuilder;
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

  /**
   * This method builds a physical DAG and tests whether the physical DAG successfully gets scheduled..
   */
  // TODO #93: Implement Batch Scheduler
  // The tests will be extended with Batch Scheduler.
  @Test
  public void testSimplePhysicalPlanScheduling() {
    final DAGBuilder<PhysicalStage, PhysicalStageEdge> builder = new DAGBuilder<>();

    final TaskGroup taskGroup1 = mock(TaskGroup.class);
    when(taskGroup1.getResourceType()).thenReturn(RuntimeAttribute.Compute);
    final TaskGroup taskGroup2 = mock(TaskGroup.class);
    when(taskGroup1.getResourceType()).thenReturn(RuntimeAttribute.Compute);
    final TaskGroup taskGroup3 = mock(TaskGroup.class);
    when(taskGroup1.getResourceType()).thenReturn(RuntimeAttribute.Compute);

    final TaskGroup taskGroup4 = mock(TaskGroup.class);
    when(taskGroup1.getResourceType()).thenReturn(RuntimeAttribute.Storage);
    final TaskGroup taskGroup5 = mock(TaskGroup.class);
    when(taskGroup1.getResourceType()).thenReturn(RuntimeAttribute.Storage);

    final TaskGroup taskGroup6 = mock(TaskGroup.class);
    when(taskGroup1.getResourceType()).thenReturn(RuntimeAttribute.DistributedStorage);
    final TaskGroup taskGroup7 = mock(TaskGroup.class);
    when(taskGroup1.getResourceType()).thenReturn(RuntimeAttribute.DistributedStorage);

    PhysicalStageBuilder physicalStageBuilder = new PhysicalStageBuilder("Stage-1", 3);
    physicalStageBuilder.addTaskGroup(taskGroup1);
    physicalStageBuilder.addTaskGroup(taskGroup2);
    physicalStageBuilder.addTaskGroup(taskGroup3);

    final PhysicalStage stage1 = physicalStageBuilder.build();
    builder.addVertex(stage1);

    physicalStageBuilder = new PhysicalStageBuilder("Stage-2", 2);
    physicalStageBuilder.addTaskGroup(taskGroup4);
    physicalStageBuilder.addTaskGroup(taskGroup5);

    final PhysicalStage stage2 = physicalStageBuilder.build();
    builder.addVertex(stage2);

    physicalStageBuilder = new PhysicalStageBuilder("Stage-3", 2);
    physicalStageBuilder.addTaskGroup(taskGroup6);
    physicalStageBuilder.addTaskGroup(taskGroup7);

    final PhysicalStage stage3 = physicalStageBuilder.build();
    builder.addVertex(stage3);

    scheduler.scheduleJob(new PhysicalPlan("TestPlan", builder.build()));
  }
}
