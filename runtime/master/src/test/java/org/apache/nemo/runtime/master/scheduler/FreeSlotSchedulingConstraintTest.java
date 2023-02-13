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

import org.apache.nemo.common.ir.vertex.executionproperty.ResourceSlotProperty;
import org.apache.nemo.common.Task;
import org.apache.nemo.runtime.master.DefaultExecutorRepresenterImpl;
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
@PrepareForTest({DefaultExecutorRepresenterImpl.class, Task.class})
public final class FreeSlotSchedulingConstraintTest {
  private SchedulingConstraint schedulingConstraint;
  private DefaultExecutorRepresenterImpl a0;
  private DefaultExecutorRepresenterImpl a1;

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
  private static DefaultExecutorRepresenterImpl mockExecutorRepresenter(final int numComplyingTasks,
                                                                        final int capacity) {
    final DefaultExecutorRepresenterImpl executorRepresenter = mock(DefaultExecutorRepresenterImpl.class);
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

    final Set<DefaultExecutorRepresenterImpl> executorRepresenterList = new HashSet<>(Arrays.asList(a0, a1));

    final Set<DefaultExecutorRepresenterImpl> candidateExecutors = executorRepresenterList.stream()
        .filter(e -> schedulingConstraint.testSchedulability(e, task))
        .collect(Collectors.toSet());

    final Set<DefaultExecutorRepresenterImpl> expectedExecutors = Collections.singleton(a1);
    assertEquals(expectedExecutors, candidateExecutors);
  }

  /**
   * Test whether a task with false compliance property is not filtered by the constraint.
   */
  @Test
  public void testIgnoringSlot() {

    final Task task = mock(Task.class);
    when(task.getPropertyValue(ResourceSlotProperty.class)).thenReturn(Optional.of(false));

    final Set<DefaultExecutorRepresenterImpl> executorRepresenterList = new HashSet<>(Arrays.asList(a0, a1));

    final Set<DefaultExecutorRepresenterImpl> candidateExecutors = executorRepresenterList.stream()
        .filter(e -> schedulingConstraint.testSchedulability(e, task))
        .collect(Collectors.toSet());

    final Set<DefaultExecutorRepresenterImpl> expectedExecutors = new HashSet<>(Arrays.asList(a0, a1));
    assertEquals(expectedExecutors, candidateExecutors);
  }
}
