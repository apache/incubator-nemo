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

import org.apache.nemo.common.ir.vertex.executionproperty.ResourcePriorityProperty;
import org.apache.nemo.runtime.common.plan.Task;
import org.apache.nemo.runtime.master.resource.ExecutorRepresenter;
import org.apache.reef.tang.Tang;
import org.apache.reef.tang.exceptions.InjectionException;
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
 * Tests {@link ContainerTypeAwareSchedulingConstraint}.
 */
@RunWith(PowerMockRunner.class)
@PrepareForTest({ExecutorRepresenter.class, Task.class})
public final class ContainerTypeAwareSchedulingConstraintTest {

  private static ExecutorRepresenter mockExecutorRepresenter(final String containerType) {
    final ExecutorRepresenter executorRepresenter = mock(ExecutorRepresenter.class);
    when(executorRepresenter.getContainerType()).thenReturn(containerType);
    return executorRepresenter;
  }

  @Test
  public void testContainerTypeAware() throws InjectionException {
    final SchedulingConstraint schedulingConstraint = Tang.Factory.getTang().newInjector()
      .getInstance(ContainerTypeAwareSchedulingConstraint.class);
    final ExecutorRepresenter a0 = mockExecutorRepresenter(ResourcePriorityProperty.TRANSIENT);
    final ExecutorRepresenter a1 = mockExecutorRepresenter(ResourcePriorityProperty.RESERVED);
    final ExecutorRepresenter a2 = mockExecutorRepresenter(ResourcePriorityProperty.NONE);

    final Task task1 = mock(Task.class);
    when(task1.getPropertyValue(ResourcePriorityProperty.class))
      .thenReturn(Optional.of(ResourcePriorityProperty.RESERVED));

    final Set<ExecutorRepresenter> executorRepresenterList1 = new HashSet<>(Arrays.asList(a0, a1, a2));

    final Set<ExecutorRepresenter> candidateExecutors1 = executorRepresenterList1.stream()
      .filter(e -> schedulingConstraint.testSchedulability(e, task1))
      .collect(Collectors.toSet());
    ;

    final Set<ExecutorRepresenter> expectedExecutors1 = Collections.singleton(a1);
    assertEquals(expectedExecutors1, candidateExecutors1);

    final Task task2 = mock(Task.class);
    when(task2.getPropertyValue(ResourcePriorityProperty.class))
      .thenReturn(Optional.of(ResourcePriorityProperty.NONE));

    final Set<ExecutorRepresenter> executorRepresenterList2 = new HashSet<>(Arrays.asList(a0, a1, a2));

    final Set<ExecutorRepresenter> candidateExecutors2 = executorRepresenterList2.stream()
      .filter(e -> schedulingConstraint.testSchedulability(e, task2))
      .collect(Collectors.toSet());

    final Set<ExecutorRepresenter> expectedExecutors2 = new HashSet<>(Arrays.asList(a0, a1, a2));
    assertEquals(expectedExecutors2, candidateExecutors2);
  }
}

