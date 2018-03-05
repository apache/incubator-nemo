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

import edu.snu.nemo.common.ir.vertex.executionproperty.ExecutorPlacementProperty;
import edu.snu.nemo.runtime.common.plan.physical.ScheduledTaskGroup;
import edu.snu.nemo.runtime.common.state.TaskGroupState;
import edu.snu.nemo.common.ir.Readable;
import edu.snu.nemo.runtime.master.JobStateManager;
import edu.snu.nemo.runtime.master.scheduler.ExecutorRegistry;
import edu.snu.nemo.runtime.master.resource.ExecutorRepresenter;
import edu.snu.nemo.runtime.master.scheduler.RoundRobinSchedulingPolicy;
import edu.snu.nemo.runtime.master.scheduler.SchedulingPolicy;
import edu.snu.nemo.runtime.master.scheduler.SourceLocationAwareSchedulingPolicy;
import org.apache.reef.tang.Injector;
import org.apache.reef.tang.Tang;
import org.apache.reef.tang.exceptions.InjectionException;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.*;

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
  private static final int SCHEDULER_TIMEOUT_MS = 500;

  private SourceLocationAwareSchedulingPolicy sourceLocationAware;
  private SpiedSchedulingPolicyWrapper<RoundRobinSchedulingPolicy> roundRobin;
  private MockJobStateManagerWrapper jobStateManager;

  @Before
  public void setup() throws InjectionException {
    final Injector injector = Tang.Factory.getTang().newInjector();
    jobStateManager = new MockJobStateManagerWrapper();

    final ExecutorRegistry executorRegistry = new ExecutorRegistry();
    final RoundRobinSchedulingPolicy roundRobinSchedulingPolicy =
        new RoundRobinSchedulingPolicy(executorRegistry, SCHEDULER_TIMEOUT_MS);
    roundRobin = new SpiedSchedulingPolicyWrapper(roundRobinSchedulingPolicy, jobStateManager.get());

    injector.bindVolatileInstance(RoundRobinSchedulingPolicy.class, roundRobin.get());
    injector.bindVolatileInstance(JobStateManager.class, jobStateManager.get());
    injector.bindVolatileInstance(ExecutorRegistry.class, executorRegistry);
    sourceLocationAware = injector.getInstance(SourceLocationAwareSchedulingPolicy.class);
  }

  @After
  public void teardown() {
    // All expectations should be resolved at this time.
    roundRobin.ensureNoUnresolvedExpectation();
  }

  /**
   * {@link SourceLocationAwareSchedulingPolicy} should delegate scheduling decision when the
   * {@link ScheduledTaskGroup} does not have any source tasks.
   */
  @Test
  public void testRoundRobinSchedulerFallback() {
    // Prepare test scenario
    final ScheduledTaskGroup tg0 = CreateScheduledTaskGroup.withoutReadables(ExecutorPlacementProperty.NONE);
    final ScheduledTaskGroup tg1 = CreateScheduledTaskGroup.withReadablesWithoutSourceLocations(2,
        ExecutorPlacementProperty.NONE);
    final ScheduledTaskGroup tg2 = CreateScheduledTaskGroup.withReadablesWhichThrowException(5,
        ExecutorPlacementProperty.NONE);
    addExecutor(new MockExecutorRepresenterWrapper(SITE_0, ExecutorPlacementProperty.NONE, 1));
    addExecutor(new MockExecutorRepresenterWrapper(SITE_1, ExecutorPlacementProperty.NONE, 1));

    // Trying to schedule tg0: expected to fall back to RoundRobinSchedulingPolicy
    roundRobin.expectSchedulingRequest(tg0);
    // ...and scheduling attempt must success
    assertTrue(sourceLocationAware.scheduleTaskGroup(tg0, jobStateManager.get()));
    // ...thus the TaskGroup should be running
    jobStateManager.assertTaskGroupState(tg0.getTaskGroupId(), TaskGroupState.State.EXECUTING);

    // Trying to schedule tg1: expected to fall back to RoundRobinSchedulingPolicy
    roundRobin.expectSchedulingRequest(tg1);
    // ...and scheduling attempt must success
    assertTrue(sourceLocationAware.scheduleTaskGroup(tg1, jobStateManager.get()));
    // ...thus the TaskGroup should be running
    jobStateManager.assertTaskGroupState(tg1.getTaskGroupId(), TaskGroupState.State.EXECUTING);

    // Trying to schedule tg2: expected to fall back to RoundRobinSchedulingPolicy
    roundRobin.expectSchedulingRequest(tg2);
    // ...and scheduling attempt must success
    assertTrue(sourceLocationAware.scheduleTaskGroup(tg2, jobStateManager.get()));
    // ...thus the TaskGroup should be running
    jobStateManager.assertTaskGroupState(tg2.getTaskGroupId(), TaskGroupState.State.EXECUTING);
  }

  /**
   * {@link SourceLocationAwareSchedulingPolicy} should fail to schedule a {@link ScheduledTaskGroup} when
   */
  @Test
  public void testSourceLocationAwareSchedulingNotAvailable() {
    // Prepare test scenario
    final ScheduledTaskGroup tg = CreateScheduledTaskGroup.withReadablesWithSourceLocations(
        Collections.singletonList(Collections.singletonList(SITE_0)), ExecutorPlacementProperty.NONE);
    final MockExecutorRepresenterWrapper e0 = addExecutor(
        new MockExecutorRepresenterWrapper(SITE_1, ExecutorPlacementProperty.NONE, 1));
    final MockExecutorRepresenterWrapper e1 = addExecutor(
        new MockExecutorRepresenterWrapper(SITE_1, ExecutorPlacementProperty.NONE, 1));

    // Attempt to schedule tg must fail (fallback to round robin policy is not expected)
    assertFalse(sourceLocationAware.scheduleTaskGroup(tg, jobStateManager.get()));
    // Thus executors should have no running TaskGroups at all
    e0.assertScheduledTaskGroups(Collections.emptyList());
    e1.assertScheduledTaskGroups(Collections.emptyList());
  }

  private static final String CONTAINER_TYPE_A = "A";

  /**
   * {@link SourceLocationAwareSchedulingPolicy} should schedule TG to one of the executors with appropriate
   * location and container type.
   */
  @Test
  public void testSourceLocationAwareSchedulingWithContainerType() {
    // Prepare test scenario
    final ScheduledTaskGroup tg = CreateScheduledTaskGroup.withReadablesWithSourceLocations(
        Collections.singletonList(Collections.singletonList(SITE_0)), CONTAINER_TYPE_A);
    final MockExecutorRepresenterWrapper e0 = addExecutor(
        new MockExecutorRepresenterWrapper(SITE_0, ExecutorPlacementProperty.NONE, 1));
    final MockExecutorRepresenterWrapper e1 = addExecutor(
        new MockExecutorRepresenterWrapper(SITE_1, ExecutorPlacementProperty.NONE, 1));
    final MockExecutorRepresenterWrapper e2 = addExecutor(
        new MockExecutorRepresenterWrapper(SITE_2, ExecutorPlacementProperty.NONE, 1));
    final MockExecutorRepresenterWrapper e3 = addExecutor(
        new MockExecutorRepresenterWrapper(SITE_0, CONTAINER_TYPE_A, 1));
    final MockExecutorRepresenterWrapper e4 = addExecutor(
        new MockExecutorRepresenterWrapper(SITE_1, CONTAINER_TYPE_A, 1));
    final MockExecutorRepresenterWrapper e5 = addExecutor(
        new MockExecutorRepresenterWrapper(SITE_2, CONTAINER_TYPE_A, 1));

    // Attempt to schedule tg must success (fallback to round robin is not expected)
    assertTrue(sourceLocationAware.scheduleTaskGroup(tg, jobStateManager.get()));
    // tg must run on e3
    e0.assertScheduledTaskGroups(Collections.emptyList());
    e1.assertScheduledTaskGroups(Collections.emptyList());
    e2.assertScheduledTaskGroups(Collections.emptyList());
    e3.assertScheduledTaskGroups(Collections.singletonList(tg));
    e4.assertScheduledTaskGroups(Collections.emptyList());
    e5.assertScheduledTaskGroups(Collections.emptyList());
  }

  /**
   * {@link SourceLocationAwareSchedulingPolicy} should not schedule more TGs than executor capacity allows.
   */
  @Test
  public void testSourceLocationAwareSchedulingDoesNotOverSchedule() {
    // Prepare test scenario
    final ScheduledTaskGroup tg0 = CreateScheduledTaskGroup.withReadablesWithSourceLocations(
        Collections.singletonList(Collections.singletonList(SITE_0)), CONTAINER_TYPE_A);
    final ScheduledTaskGroup tg1 = CreateScheduledTaskGroup.withReadablesWithSourceLocations(
        Collections.singletonList(Collections.singletonList(SITE_0)), CONTAINER_TYPE_A);
    final ScheduledTaskGroup tg2 = CreateScheduledTaskGroup.withReadablesWithSourceLocations(
        Collections.singletonList(Collections.singletonList(SITE_0)), CONTAINER_TYPE_A);
    final ScheduledTaskGroup tg3 = CreateScheduledTaskGroup.withReadablesWithSourceLocations(
        Collections.singletonList(Collections.singletonList(SITE_0)), CONTAINER_TYPE_A);
    final MockExecutorRepresenterWrapper e = addExecutor(
        new MockExecutorRepresenterWrapper(SITE_0, CONTAINER_TYPE_A, 3));

    // Attempt to schedule TG must success (fallback to round robin is not expected)
    assertTrue(sourceLocationAware.scheduleTaskGroup(tg0, jobStateManager.get()));
    assertTrue(sourceLocationAware.scheduleTaskGroup(tg1, jobStateManager.get()));
    assertTrue(sourceLocationAware.scheduleTaskGroup(tg2, jobStateManager.get()));

    // This must fail
    assertFalse(sourceLocationAware.scheduleTaskGroup(tg3, jobStateManager.get()));

    // Expected executor status
    e.assertScheduledTaskGroups(Arrays.asList(tg0, tg1, tg2));
  }

  /**
   * {@link SourceLocationAwareSchedulingPolicy} should properly schedule TGs with multiple source locations.
   */
  @Test
  public void testSourceLocationAwareSchedulingWithMultiSource() {
    // Prepare test scenario
    final ScheduledTaskGroup tg0 = CreateScheduledTaskGroup.withReadablesWithSourceLocations(
        Collections.singletonList(Collections.singletonList(SITE_1)), CONTAINER_TYPE_A);
    final ScheduledTaskGroup tg1 = CreateScheduledTaskGroup.withReadablesWithSourceLocations(
        Collections.singletonList(Arrays.asList(SITE_0, SITE_1, SITE_2)), CONTAINER_TYPE_A);
    final ScheduledTaskGroup tg2 = CreateScheduledTaskGroup.withReadablesWithSourceLocations(
        Arrays.asList(Collections.singletonList(SITE_0), Collections.singletonList(SITE_1),
            Arrays.asList(SITE_1, SITE_2)), CONTAINER_TYPE_A);
    final ScheduledTaskGroup tg3 = CreateScheduledTaskGroup.withReadablesWithSourceLocations(
        Arrays.asList(Collections.singletonList(SITE_1), Collections.singletonList(SITE_0),
            Arrays.asList(SITE_0, SITE_2)), CONTAINER_TYPE_A);
    final MockExecutorRepresenterWrapper e = addExecutor(
        new MockExecutorRepresenterWrapper(SITE_1, CONTAINER_TYPE_A, 4));

    // Attempt to schedule TG must success (fallback to round robin is not expected)
    assertTrue(sourceLocationAware.scheduleTaskGroup(tg0, jobStateManager.get()));
    assertTrue(sourceLocationAware.scheduleTaskGroup(tg1, jobStateManager.get()));
    assertTrue(sourceLocationAware.scheduleTaskGroup(tg2, jobStateManager.get()));
    assertTrue(sourceLocationAware.scheduleTaskGroup(tg3, jobStateManager.get()));

    // Expected executor status
    e.assertScheduledTaskGroups(Arrays.asList(tg0, tg1, tg2, tg3));
  }

  private MockExecutorRepresenterWrapper addExecutor(final MockExecutorRepresenterWrapper executor) {
    sourceLocationAware.onExecutorAdded(executor.get());
    return executor;
  }

  /**
   * Utility for creating {@link ScheduledTaskGroup}.
   */
  private static final class CreateScheduledTaskGroup {
    private static final AtomicInteger taskGroupIndex = new AtomicInteger(0);
    private static final AtomicInteger taskIndex = new AtomicInteger(0);

    private static ScheduledTaskGroup doCreate(final Collection<Readable> readables, final String containerType) {
      final ScheduledTaskGroup mockInstance = mock(ScheduledTaskGroup.class);
      final Map<String, Readable> readableMap = new HashMap<>();
      readables.forEach(readable -> readableMap.put(String.format("TASK-%d", taskIndex.getAndIncrement()),
          readable));
      when(mockInstance.getTaskGroupId()).thenReturn(String.format("TG-%d", taskGroupIndex.getAndIncrement()));
      when(mockInstance.getLogicalTaskIdToReadable()).thenReturn(readableMap);
      when(mockInstance.getContainerType()).thenReturn(containerType);
      return mockInstance;
    }

    static ScheduledTaskGroup withReadablesWithSourceLocations(final Collection<List<String>> sourceLocation,
                                                               final String containerType) {
      try {
        final List<Readable> readables = new ArrayList<>();
        for (final List<String> locations : sourceLocation) {
          final Readable readable = mock(Readable.class);
          when(readable.getLocations()).thenReturn(locations);
          readables.add(readable);
        }
        return doCreate(readables, containerType);
      } catch (final Exception e) {
        throw new RuntimeException(e);
      }
    }

    static ScheduledTaskGroup withReadablesWithoutSourceLocations(final int numReadables,
                                                                  final String containerType) {
      try {
        final List<Readable> readables = new ArrayList<>();
        for (int i = 0; i < numReadables; i++) {
          final Readable readable = mock(Readable.class);
          when(readable.getLocations()).thenReturn(Collections.emptyList());
          readables.add(readable);
        }
        return doCreate(readables, containerType);
      } catch (final Exception e) {
        throw new RuntimeException(e);
      }
    }

    static ScheduledTaskGroup withReadablesWhichThrowException(final int numReadables,
                                                               final String containerType) {
      try {
        final List<Readable> readables = new ArrayList<>();
        for (int i = 0; i < numReadables; i++) {
          final Readable readable = mock(Readable.class);
          when(readable.getLocations()).thenThrow(new Exception("EXCEPTION"));
          readables.add(readable);
        }
        return doCreate(readables, containerType);
      } catch (final Exception e) {
        throw new RuntimeException(e);
      }
    }

    static ScheduledTaskGroup withoutReadables(final String containerType) {
      return doCreate(Collections.emptyList(), containerType);
    }
  }

  /**
   * Wrapper for mock {@link ExecutorRepresenter}.
   */
  private static final class MockExecutorRepresenterWrapper {
    private static final AtomicInteger executorIndex = new AtomicInteger(0);

    private final ExecutorRepresenter mockInstance;
    private final List<ScheduledTaskGroup> scheduledTaskGroups = new ArrayList<>();

    MockExecutorRepresenterWrapper(final String nodeName, final String containerType, final int capacity) {
      mockInstance = mock(ExecutorRepresenter.class);
      doAnswer(invocationOnMock -> {
        final ScheduledTaskGroup scheduledTaskGroup = invocationOnMock.getArgument(0);
        scheduledTaskGroups.add(scheduledTaskGroup);
        return null;
      }).when(mockInstance).onTaskGroupScheduled(any(ScheduledTaskGroup.class));
      when(mockInstance.getExecutorId()).thenReturn(String.format("EXECUTOR-%d", executorIndex.getAndIncrement()));
      when(mockInstance.getNodeName()).thenReturn(nodeName);
      when(mockInstance.getContainerType()).thenReturn(containerType);
      doAnswer(invocationOnMock ->
          scheduledTaskGroups.stream().map(ScheduledTaskGroup::getTaskGroupId).collect(Collectors.toSet()))
          .when(mockInstance).getRunningTaskGroups();
      when(mockInstance.getExecutorCapacity()).thenReturn(capacity);
    }

    void assertScheduledTaskGroups(final List<ScheduledTaskGroup> expected) {
      assertEquals(expected, scheduledTaskGroups);
    }

    ExecutorRepresenter get() {
      return mockInstance;
    }
  }

  /**
   * Wrapper for spied {@link SchedulingPolicy}.
   * @param <T> the class of the spied instance
   */
  private static final class SpiedSchedulingPolicyWrapper<T extends SchedulingPolicy> {
    private final T spiedInstance;
    private ScheduledTaskGroup expectedArgument = null;

    SpiedSchedulingPolicyWrapper(final T schedulingPolicy, final JobStateManager jobStateManager) {
      spiedInstance = spy(schedulingPolicy);
      doAnswer(invocationOnMock -> {
        final ScheduledTaskGroup scheduledTaskGroup = invocationOnMock.getArgument(0);
        assertEquals(expectedArgument, scheduledTaskGroup);
        expectedArgument = null;
        jobStateManager.onTaskGroupStateChanged(scheduledTaskGroup.getTaskGroupId(), TaskGroupState.State.EXECUTING);
        return true;
      }).when(spiedInstance).scheduleTaskGroup(any(ScheduledTaskGroup.class), any());
    }

    /**
     * Sets expected {@link SchedulingPolicy#scheduleTaskGroup(ScheduledTaskGroup, JobStateManager)} invocation
     * on this spied object.
     * @param scheduledTaskGroup expected parameter for the task group to schedule
     */
    void expectSchedulingRequest(final ScheduledTaskGroup scheduledTaskGroup) {
      ensureNoUnresolvedExpectation();
      this.expectedArgument = scheduledTaskGroup;
    }

    void ensureNoUnresolvedExpectation() {
      assertEquals(null, expectedArgument);
    }

    /**
     * @return spied instance for {@link SchedulingPolicy}.
     */
    T get() {
      return spiedInstance;
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
