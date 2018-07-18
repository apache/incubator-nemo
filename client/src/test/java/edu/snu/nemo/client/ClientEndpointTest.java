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
package edu.snu.nemo.client;

import edu.snu.nemo.runtime.common.plan.PhysicalPlan;
import edu.snu.nemo.runtime.common.state.JobState;
import edu.snu.nemo.runtime.common.state.TaskState;
import edu.snu.nemo.runtime.master.MetricMessageHandler;
import edu.snu.nemo.runtime.master.JobStateManager;
import edu.snu.nemo.runtime.common.plan.TestPlanGenerator;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static org.junit.Assert.assertEquals;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Test {@link ClientEndpoint}.
 */
@RunWith(PowerMockRunner.class)
@PrepareForTest(MetricMessageHandler.class)
public class ClientEndpointTest {
  private static final int MAX_SCHEDULE_ATTEMPT = 2;
  private final MetricMessageHandler metricMessageHandler = mock(MetricMessageHandler.class);

  @Test(timeout = 3000)
  public void testState() throws Exception {
    // Create a simple client endpoint that returns given job state.
    final StateTranslator stateTranslator = mock(StateTranslator.class);
    when(stateTranslator.translateState(any())).then(state -> state.getArgument(0));
    final ClientEndpoint clientEndpoint = new TestClientEndpoint(stateTranslator);
    assertEquals(clientEndpoint.getJobState(), JobState.State.READY);

    // Wait for connection but not connected.
    assertEquals(clientEndpoint.waitUntilJobFinish(100, TimeUnit.MILLISECONDS), JobState.State.READY);

    // Create a JobStateManager of a dag and create a DriverEndpoint with it.
    final PhysicalPlan physicalPlan =
        TestPlanGenerator.generatePhysicalPlan(TestPlanGenerator.PlanType.TwoVerticesJoined, false);
    final JobStateManager jobStateManager =
        new JobStateManager(physicalPlan, metricMessageHandler, MAX_SCHEDULE_ATTEMPT);

    final DriverEndpoint driverEndpoint = new DriverEndpoint(jobStateManager, clientEndpoint);

    // Check the current state.
    assertEquals(clientEndpoint.getJobState(), JobState.State.EXECUTING);

    // Wait for the job to finish but not finished
    assertEquals(clientEndpoint.waitUntilJobFinish(100, TimeUnit.MILLISECONDS), JobState.State.EXECUTING);

    // Check finish.
    final List<String> tasks = physicalPlan.getStageDAG().getTopologicalSort().stream()
        .flatMap(stage -> stage.getTaskIds().stream())
        .collect(Collectors.toList());
    tasks.forEach(taskId -> jobStateManager.onTaskStateChanged(taskId, TaskState.State.EXECUTING));
    tasks.forEach(taskId -> jobStateManager.onTaskStateChanged(taskId, TaskState.State.COMPLETE));
    assertEquals(JobState.State.COMPLETE, clientEndpoint.waitUntilJobFinish());
  }

  /**
   * A simple {@link ClientEndpoint} for test.
   */
  private final class TestClientEndpoint extends ClientEndpoint {

    TestClientEndpoint(final StateTranslator stateTranslator) {
      super(stateTranslator);
    }
  }
}
