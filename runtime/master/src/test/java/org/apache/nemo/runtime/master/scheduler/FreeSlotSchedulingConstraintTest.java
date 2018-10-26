package org.apache.nemo.runtime.master.scheduler;

import org.apache.nemo.common.ir.vertex.executionproperty.ResourceSlotProperty;
import org.apache.nemo.runtime.common.plan.Task;
import org.apache.nemo.runtime.master.resource.ExecutorRepresenter;
import org.apache.reef.tang.Tang;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.util.*;
import java.util.stream.Collectors;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.*;

/**
 * Tests {@link FreeSlotSchedulingConstraint}.
 */
@RunWith(PowerMockRunner.class)
@PrepareForTest({ExecutorRepresenter.class, Task.class})
public final class FreeSlotSchedulingConstraintTest {
  private SchedulingConstraint schedulingConstraint;
  private ExecutorRepresenter a0;
  private ExecutorRepresenter a1;

  @Before
  public void setUp() throws Exception {
    schedulingConstraint = Tang.Factory.getTang().newInjector().getInstance(FreeSlotSchedulingConstraint.class);
    a0 = mockExecutorRepresenter(1, 1);
    a1 = mockExecutorRepresenter(2, 3);
  }

  /**
   * Mock an executor representer.
   *
   * @param numComplyingTasks the number of already running (mocked) tasks which comply slot constraint in the executor.
   * @param capacity          the capacity of the executor.
   * @return the mocked executor.
   */
  private static ExecutorRepresenter mockExecutorRepresenter(final int numComplyingTasks,
                                                             final int capacity) {
    final ExecutorRepresenter executorRepresenter = mock(ExecutorRepresenter.class);
    when(executorRepresenter.getNumOfComplyingRunningTasks()).thenReturn(numComplyingTasks);
    when(executorRepresenter.getExecutorCapacity()).thenReturn(capacity);
    return executorRepresenter;
  }

  /**
   * Test whether the constraint filter full executors.
   */
  @Test
  public void testFreeSlot() {

    final Task task = mock(Task.class);
    when(task.getPropertyValue(ResourceSlotProperty.class)).thenReturn(Optional.of(true));

    final Set<ExecutorRepresenter> executorRepresenterList = new HashSet<>(Arrays.asList(a0, a1));

    final Set<ExecutorRepresenter> candidateExecutors = executorRepresenterList.stream()
        .filter(e -> schedulingConstraint.testSchedulability(e, task))
        .collect(Collectors.toSet());

    final Set<ExecutorRepresenter> expectedExecutors = Collections.singleton(a1);
    assertEquals(expectedExecutors, candidateExecutors);
  }

  /**
   * Test whether a task with false compliance property is not filtered by the constraint.
   */
  @Test
  public void testIgnoringSlot() {

    final Task task = mock(Task.class);
    when(task.getPropertyValue(ResourceSlotProperty.class)).thenReturn(Optional.of(false));

    final Set<ExecutorRepresenter> executorRepresenterList = new HashSet<>(Arrays.asList(a0, a1));

    final Set<ExecutorRepresenter> candidateExecutors = executorRepresenterList.stream()
        .filter(e -> schedulingConstraint.testSchedulability(e, task))
        .collect(Collectors.toSet());

    final Set<ExecutorRepresenter> expectedExecutors = new HashSet<>(Arrays.asList(a0, a1));
    assertEquals(expectedExecutors, candidateExecutors);
  }
}
