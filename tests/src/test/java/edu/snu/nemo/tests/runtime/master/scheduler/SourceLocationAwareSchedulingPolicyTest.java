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
package edu.snu.nemo.tests.runtime.master.scheduler;

import edu.snu.nemo.runtime.common.plan.physical.ScheduledTaskGroup;
import edu.snu.nemo.runtime.common.state.TaskGroupState;
import edu.snu.nemo.runtime.master.JobStateManager;
import edu.snu.nemo.runtime.master.resource.ExecutorRepresenter;
import edu.snu.nemo.runtime.master.scheduler.RoundRobinSchedulingPolicy;
import edu.snu.nemo.runtime.master.scheduler.SchedulingPolicy;
import edu.snu.nemo.runtime.master.scheduler.SourceLocationAwareSchedulingPolicy;
import org.apache.reef.tang.Injector;
import org.apache.reef.tang.Tang;
import org.apache.reef.tang.exceptions.InjectionException;
import org.junit.Before;
import org.junit.runner.RunWith;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;

/**
 * Test cases for
 */
@RunWith(PowerMockRunner.class)
@PrepareForTest({JobStateManager.class, RoundRobinSchedulingPolicy.class})
public final class SourceLocationAwareSchedulingPolicyTest {
  private SourceLocationAwareSchedulingPolicy sourceLocationAware;
  private MockSchedulingPolicyWrapper<RoundRobinSchedulingPolicy> roundRobin;
  private MockJobStateManagerWrapper jobStateManager;

  @Before
  public void setup() throws InjectionException {
    final Injector injector = Tang.Factory.getTang().newInjector();
    roundRobin = new MockSchedulingPolicyWrapper(RoundRobinSchedulingPolicy.class);
    jobStateManager = new MockJobStateManagerWrapper();
    injector.bindVolatileInstance(RoundRobinSchedulingPolicy.class, roundRobin.get());
    injector.bindVolatileInstance(JobStateManager.class, jobStateManager.get());
    sourceLocationAware = injector.getInstance(SourceLocationAwareSchedulingPolicy.class);
  }

  private static ExecutorRepresenter createExecutorRepresenter(final String executorId) {

  }

  /**
   * Wrapper for mock {@link SchedulingPolicy}.
   * @param <T> the class of the mocked instance
   */
  private final class MockSchedulingPolicyWrapper<T extends SchedulingPolicy> {
    private final T mockInstance;

    private ScheduledTaskGroup expectedArgument = null;
    private Boolean predeterminedAnswer = null;

    MockSchedulingPolicyWrapper(final Class<T> schedulingPolicyClass) {
      mockInstance = mock(schedulingPolicyClass);
      doAnswer(invocationOnMock -> {
        final ScheduledTaskGroup scheduledTaskGroup = invocationOnMock.getArgument(0);
        assertEquals(scheduledTaskGroup, expectedArgument);
        final boolean answer = predeterminedAnswer;
        expectedArgument = null;
        predeterminedAnswer = null;
        return answer;
      }).when(mockInstance).scheduleTaskGroup(any(ScheduledTaskGroup.class), any());
    }

    /**
     * Sets expected {@link SchedulingPolicy#scheduleTaskGroup(ScheduledTaskGroup, JobStateManager)} invocation
     * on this mock object.
     * @param scheduledTaskGroup expected parameter for the task group to schedule
     * @param hasScheduled predetermined return value, which is true if the task group is successfully scheduled,
     *                     and false otherwise.
     */
    void expect(final ScheduledTaskGroup scheduledTaskGroup, final boolean hasScheduled) {
      this.expectedArgument = scheduledTaskGroup;
      this.predeterminedAnswer = hasScheduled;
    }

    /**
     * @return mock instance for {@link SchedulingPolicy}.
     */
    T get() {
      return mockInstance;
    }
  }

  /**
   * Wrapper for mock {@link JobStateManager} instance.
   */
  private final class MockJobStateManagerWrapper {
    private final JobStateManager mockInstance;
    private final Map<String, TaskGroupState.State> taskGroupStates = new HashMap<>();

    MockJobStateManagerWrapper() {
      mockInstance = mock(JobStateManager.class);
      doAnswer(invocationOnMock -> {
        final String taskGroupId = invocationOnMock.getArgument(0);
        final TaskGroupState.State newState = invocationOnMock.getArgument(1);
        taskGroupStates.put(taskGroupId, newState);
        return null;
      }).when(mockInstance).onTaskGroupStateChanged(anyString(), any(TaskGroupState.State.class));
    }

    /**
     * Ensures the TaskGroup state has been changed as expected.
     * @param taskGroupId id of the TaskGroup
     * @param state the expected state
     */
    void assertTaskGroupState(final String taskGroupId, final TaskGroupState.State state) {
      assertEquals(taskGroupStates.get(taskGroupId), state);
    }

    /**
     * @return mock instance for {@link JobStateManager}.
     */
    JobStateManager get() {
      return mockInstance;
    }
  }
}
