package org.apache.nemo.client;

import org.apache.nemo.runtime.common.plan.PhysicalPlan;
import org.apache.nemo.runtime.common.state.PlanState;
import org.apache.nemo.runtime.common.state.TaskState;
import org.apache.nemo.runtime.master.MetricMessageHandler;
import org.apache.nemo.runtime.master.PlanStateManager;
import org.apache.nemo.runtime.common.plan.TestPlanGenerator;
import org.apache.reef.tang.Injector;
import org.apache.reef.tang.Tang;
import org.junit.Test;
import org.junit.runner.RunWith;
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
public class ClientEndpointTest {
  private static final int MAX_SCHEDULE_ATTEMPT = 2;

  @Test(timeout = 10000)
  public void testState() throws Exception {
    // Create a simple client endpoint that returns given job state.
    final StateTranslator stateTranslator = mock(StateTranslator.class);
    when(stateTranslator.translateState(any())).then(state -> state.getArgument(0));
    final ClientEndpoint clientEndpoint = new TestClientEndpoint(stateTranslator);
    assertEquals(clientEndpoint.getPlanState(), PlanState.State.READY);

    // Wait for connection but not connected.
    assertEquals(clientEndpoint.waitUntilJobFinish(100, TimeUnit.MILLISECONDS), PlanState.State.READY);

    // Create a PlanStateManager of a dag and create a DriverEndpoint with it.
    final PhysicalPlan physicalPlan =
        TestPlanGenerator.generatePhysicalPlan(TestPlanGenerator.PlanType.TwoVerticesJoined, false);
    final Injector injector = Tang.Factory.getTang().newInjector();
    injector.bindVolatileInstance(MetricMessageHandler.class, mock(MetricMessageHandler.class));
    final PlanStateManager planStateManager = injector.getInstance(PlanStateManager.class);
    planStateManager.updatePlan(physicalPlan, MAX_SCHEDULE_ATTEMPT);

    final DriverEndpoint driverEndpoint = new DriverEndpoint(planStateManager, clientEndpoint);

    // Check the current state.
    assertEquals(clientEndpoint.getPlanState(), PlanState.State.EXECUTING);

    // Wait for the job to finish but not finished
    assertEquals(clientEndpoint.waitUntilJobFinish(100, TimeUnit.MILLISECONDS), PlanState.State.EXECUTING);

    // Check finish.
    final List<String> tasks = physicalPlan.getStageDAG().getTopologicalSort().stream()
        .flatMap(stage -> planStateManager.getTaskAttemptsToSchedule(stage.getId()).stream())
        .collect(Collectors.toList());
    tasks.forEach(taskId -> planStateManager.onTaskStateChanged(taskId, TaskState.State.EXECUTING));
    tasks.forEach(taskId -> planStateManager.onTaskStateChanged(taskId, TaskState.State.COMPLETE));
    assertEquals(PlanState.State.COMPLETE, clientEndpoint.waitUntilJobFinish());
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
