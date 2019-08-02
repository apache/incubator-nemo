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
import org.apache.nemo.runtime.common.plan.Task;
import org.apache.nemo.runtime.master.resource.DefaultExecutorRepresenter;
import org.apache.reef.tang.Tang;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.util.*;
import java.util.stream.Collectors;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Tests {@link FreeSlotSchedulingConstraint}.
 */
@RunWith(PowerMockRunner.class)
@PrepareForTest({DefaultExecutorRepresenter.class, Task.class})
public final class FreeSlotSchedulingConstraintTest {
  private SchedulingConstraint schedulingConstraint;
  private DefaultExecutorRepresenter a0;
  private DefaultExecutorRepresenter a1;

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
  private static DefaultExecutorRepresenter mockExecutorRepresenter(final int numComplyingTasks,
                                                                    final int capacity) {
    final DefaultExecutorRepresenter defaultExecutorRepresenter = mock(DefaultExecutorRepresenter.class);
    when(defaultExecutorRepresenter.getNumOfComplyingRunningTasks()).thenReturn(numComplyingTasks);
    when(defaultExecutorRepresenter.getExecutorCapacity()).thenReturn(capacity);
    return defaultExecutorRepresenter;
  }

  /**
   * Test whether the constraint filter full executors.
   */
  @Test
  public void testFreeSlot() {

    final Task task = mock(Task.class);
    when(task.getPropertyValue(ResourceSlotProperty.class)).thenReturn(Optional.of(true));

    final Set<DefaultExecutorRepresenter> defaultExecutorRepresenterList = new HashSet<>(Arrays.asList(a0, a1));

    final Set<DefaultExecutorRepresenter> candidateExecutors = defaultExecutorRepresenterList.stream()
      .filter(e -> schedulingConstraint.testSchedulability(e, task))
      .collect(Collectors.toSet());

    final Set<DefaultExecutorRepresenter> expectedExecutors = Collections.singleton(a1);
    assertEquals(expectedExecutors, candidateExecutors);
  }

  /**
   * Test whether a task with false compliance property is not filtered by the constraint.
   */
  @Test
  public void testIgnoringSlot() {

    final Task task = mock(Task.class);
    when(task.getPropertyValue(ResourceSlotProperty.class)).thenReturn(Optional.of(false));

    final Set<DefaultExecutorRepresenter> defaultExecutorRepresenterList = new HashSet<>(Arrays.asList(a0, a1));

    final Set<DefaultExecutorRepresenter> candidateExecutors = defaultExecutorRepresenterList.stream()
      .filter(e -> schedulingConstraint.testSchedulability(e, task))
      .collect(Collectors.toSet());

    final Set<DefaultExecutorRepresenter> expectedExecutors = new HashSet<>(Arrays.asList(a0, a1));
    assertEquals(expectedExecutors, candidateExecutors);
  }
}
