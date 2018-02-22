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
import edu.snu.nemo.common.ir.Readable;
import edu.snu.nemo.runtime.master.JobStateManager;
import edu.snu.nemo.runtime.master.resource.ExecutorRegistry;
import edu.snu.nemo.runtime.master.resource.ExecutorRepresenter;
import edu.snu.nemo.runtime.master.scheduler.RoundRobinSchedulingPolicy;
import edu.snu.nemo.runtime.master.scheduler.SchedulingPolicy;
import edu.snu.nemo.runtime.master.scheduler.SourceLocationAwareSchedulingPolicy;
import org.apache.reef.tang.Injector;
import org.apache.reef.tang.Tang;
import org.apache.reef.tang.exceptions.InjectionException;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Test cases for
 */
@RunWith(PowerMockRunner.class)
@PrepareForTest({JobStateManager.class, ExecutorRepresenter.class, RoundRobinSchedulingPolicy.class,
    ScheduledTaskGroup.class, Readable.class})
public final class SourceLocationAwareSchedulingPolicyTest {
  private static final String SITE_0 = "SEOUL";
  private static final String SITE_1 = "JINJU";
  private static final String SITE_2 = "BUSAN";

  private SourceLocationAwareSchedulingPolicy sourceLocationAware;
  private MockSchedulingPolicyWrapper<RoundRobinSchedulingPolicy> roundRobin;
  private MockJobStateManagerWrapper jobStateManager;
  private ExecutorRegistry executorRegistry;

  @Before
  public void setup() throws InjectionException {
    final Injector injector = Tang.Factory.getTang().newInjector();
    roundRobin = new MockSchedulingPolicyWrapper(RoundRobinSchedulingPolicy.class);
    jobStateManager = new MockJobStateManagerWrapper();
    injector.bindVolatileInstance(RoundRobinSchedulingPolicy.class, roundRobin.get());
    injector.bindVolatileInstance(JobStateManager.class, jobStateManager.get());
    sourceLocationAware = injector.getInstance(SourceLocationAwareSchedulingPolicy.class);
    executorRegistry = injector.getInstance(ExecutorRegistry.class);
  }

  /**
   * {@link SourceLocationAwareSchedulingPolicy} should delegate scheduling decision when the
   * {@link ScheduledTaskGroup} does not have any source tasks.
   */
  @Test
  public void testRoundRobinSchedulerFallback() {
    // Prepare test scenario
    final ScheduledTaskGroup tg0 = CreateScheduledTaskGroup.withoutReadables();
    final ScheduledTaskGroup tg1 = CreateScheduledTaskGroup.withoutReadables();
    final ScheduledTaskGroup tg2 = CreateScheduledTaskGroup.withoutReadables();
    final MockExecutorRepresenterWrapper e0 = new MockExecutorRepresenterWrapper(SITE_0);
    final MockExecutorRepresenterWrapper e1 = new MockExecutorRepresenterWrapper(SITE_1);
    addExecutor(new MockExecutorRepresenterWrapper[]{e0, e1});

    // Trying to schedule tg0: expected to fall back to RoundRobinSchedulingPolicy
    roundRobin.expect(tg0);
    sourceLocationAware.scheduleTaskGroup(tg0, jobStateManager.get());

    // Trying to schedule tg1: expected to fall back to RoundRobinSchedulingPolicy
    roundRobin.expect(tg1);
    sourceLocationAware.scheduleTaskGroup(tg1, jobStateManager.get());

    // Trying to schedule tg2: expected to fall back to RoundRobinSchedulingPolicy
    roundRobin.expect(tg2);
    sourceLocationAware.scheduleTaskGroup(tg2, jobStateManager.get());
  }

  private void addExecutor(final MockExecutorRepresenterWrapper[] executors) {
    for (final MockExecutorRepresenterWrapper executor : executors) {
      executorRegistry.registerRepresenter(executor.get());
      sourceLocationAware.onExecutorAdded(executor.get().getExecutorId());
    }
  }

  /**
   * Utility for creating {@link ScheduledTaskGroup}.
   */
  private static final class CreateScheduledTaskGroup {
    private static final AtomicInteger taskIndex = new AtomicInteger(0);

    private static ScheduledTaskGroup doCreate(final Collection<Readable> readables) {
      final ScheduledTaskGroup mockInstance = mock(ScheduledTaskGroup.class);
      final Map<String, Readable> readableMap = new HashMap<>();
      readables.forEach(readable -> readableMap.put(String.format("TASK-%d", taskIndex.getAndIncrement()),
          readable));
      when(mockInstance.getLogicalTaskIdToReadable()).thenReturn(readableMap);
      return mockInstance;
    }

    static ScheduledTaskGroup withReadablesWithSourceLocations(final Collection<List<String>> sourceLocation) {
      try {
        final List<Readable> readables = new ArrayList<>();
        for (final List<String> locations : sourceLocation) {
          final Readable readable = mock(Readable.class);
          when(readable.getLocations()).thenReturn(locations);
          readables.add(readable);
        }
        return doCreate(readables);
      } catch (final Exception e) {
        throw new RuntimeException(e);
      }
    }

    static ScheduledTaskGroup withReadablesWithoutSourceLocations(final int numReadables) {
      try {
        final List<Readable> readables = new ArrayList<>();
        for (int i = 0; i < numReadables; i++) {
          final Readable readable = mock(Readable.class);
          when(readable.getLocations()).thenReturn(Collections.emptyList());
          readables.add(readable);
        }
        return doCreate(readables);
      } catch (final Exception e) {
        throw new RuntimeException(e);
      }
    }

    static ScheduledTaskGroup withoutReadables() {
      return doCreate(Collections.emptyList());
    }
  }

  /**
   * Wrapper for mock {@link ExecutorRepresenter}.
   */
  private static final class MockExecutorRepresenterWrapper {
    private static final AtomicInteger executorIndex = new AtomicInteger(0);

    private final ExecutorRepresenter mockInstance;
    private final List<ScheduledTaskGroup> scheduledTaskGroups = new ArrayList<>();

    MockExecutorRepresenterWrapper(final String nodeName) {
      mockInstance = mock(ExecutorRepresenter.class);
      doAnswer(invocationOnMock -> {
        final ScheduledTaskGroup scheduledTaskGroup = invocationOnMock.getArgument(0);
        scheduledTaskGroups.add(scheduledTaskGroup);
        return null;
      }).when(mockInstance).onTaskGroupScheduled(any(ScheduledTaskGroup.class));
      when(mockInstance.getExecutorId()).thenReturn(String.format("EXECUTOR-%d", executorIndex.getAndIncrement()));
      when(mockInstance.getNodeName()).thenReturn(nodeName);
    }

    List<ScheduledTaskGroup> getScheduledTaskGroups() {
      return scheduledTaskGroups;
    }

    ExecutorRepresenter get() {
      return mockInstance;
    }
  }

  /**
   * Wrapper for mock {@link SchedulingPolicy}.
   * @param <T> the class of the mocked instance
   */
  private static final class MockSchedulingPolicyWrapper<T extends SchedulingPolicy> {
    private final T mockInstance;

    private ScheduledTaskGroup expectedArgument = null;

    MockSchedulingPolicyWrapper(final Class<T> schedulingPolicyClass) {
      mockInstance = mock(schedulingPolicyClass);
      doAnswer(invocationOnMock -> {
        final ScheduledTaskGroup scheduledTaskGroup = invocationOnMock.getArgument(0);
        assertEquals(expectedArgument, scheduledTaskGroup);
        expectedArgument = null;
        return true;
      }).when(mockInstance).scheduleTaskGroup(any(ScheduledTaskGroup.class), any());
    }

    /**
     * Sets expected {@link SchedulingPolicy#scheduleTaskGroup(ScheduledTaskGroup, JobStateManager)} invocation
     * on this mock object.
     * @param scheduledTaskGroup expected parameter for the task group to schedule
     */
    void expect(final ScheduledTaskGroup scheduledTaskGroup) {
      this.expectedArgument = scheduledTaskGroup;
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
  private static final class MockJobStateManagerWrapper {
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
      assertEquals(state, taskGroupStates.get(taskGroupId));
    }

    /**
     * @return mock instance for {@link JobStateManager}.
     */
    JobStateManager get() {
      return mockInstance;
    }
  }
}
