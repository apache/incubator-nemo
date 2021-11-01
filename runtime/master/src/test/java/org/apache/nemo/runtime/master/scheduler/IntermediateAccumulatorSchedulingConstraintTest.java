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

import org.apache.nemo.common.ir.vertex.executionproperty.ShuffleExecutorSetProperty;
import org.apache.nemo.runtime.common.plan.Task;
import org.apache.nemo.runtime.master.resource.ExecutorRepresenter;
import org.apache.reef.tang.Tang;
import org.apache.reef.tang.exceptions.InjectionException;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.util.*;
import java.util.concurrent.Executor;
import java.util.stream.Collectors;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Test cases for {@link IntermediateAccumulatorSchedulingConstraint}.
 */
@RunWith(PowerMockRunner.class)
@PrepareForTest({ExecutorRepresenter.class, Task.class})
public class IntermediateAccumulatorSchedulingConstraintTest {

  private static ExecutorRepresenter mockExecutorRepresenter(final String nodeName) {
    final ExecutorRepresenter executorRepresenter = mock(ExecutorRepresenter.class);
    when(executorRepresenter.getNodeName()).thenReturn(nodeName);
    return executorRepresenter;
  }

  @Test
  public void testIntermediateAccumulator() throws InjectionException {
    final SchedulingConstraint schedulingConstraint = Tang.Factory.getTang().newInjector()
      .getInstance(IntermediateAccumulatorSchedulingConstraint.class);

    final ExecutorRepresenter e0 = mockExecutorRepresenter("mulan-0");
    final ExecutorRepresenter e1 = mockExecutorRepresenter("mulan-0");
    final ExecutorRepresenter e2 = mockExecutorRepresenter("mulan-1");
    final ExecutorRepresenter e3 = mockExecutorRepresenter("mulan-1");
    final ExecutorRepresenter e4 = mockExecutorRepresenter("mulan-2");
    final ExecutorRepresenter e5 = mockExecutorRepresenter("mulan-2");
    final Set<ExecutorRepresenter> executorRepresenters = new HashSet<>(Arrays.asList(e0, e1, e2, e3, e4, e5));
    final Set<ExecutorRepresenter> expectedExecutors1 = new HashSet<>(Arrays.asList(e0, e1, e2, e3));
    final Set<ExecutorRepresenter> expectedExecutors2 = new HashSet<>(Arrays.asList(e4, e5));

    final Task task1 = mock(Task.class);
    final Task task2 = mock(Task.class);
    final Task task3 = mock(Task.class);
    ArrayList<HashSet<String>> setsOfExecutors = new ArrayList<>(Arrays.asList(
      new HashSet<>(Arrays.asList("mulan-0", "mulan-1")),
      new HashSet<>(Arrays.asList("mulan-2"))
    ));
    when(task1.getPropertyValue(ShuffleExecutorSetProperty.class)).thenReturn(Optional.of(setsOfExecutors));
    when(task1.getTaskIdx()).thenReturn(0);
    when(task2.getPropertyValue(ShuffleExecutorSetProperty.class)).thenReturn(Optional.of(setsOfExecutors));
    when(task2.getTaskIdx()).thenReturn(3);
    when(task3.getPropertyValue(ShuffleExecutorSetProperty.class)).thenReturn(Optional.of(setsOfExecutors));
    when(task3.getTaskIdx()).thenReturn(6);

    final Set<ExecutorRepresenter> candidateExecutors1 = executorRepresenters.stream()
      .filter(e -> schedulingConstraint.testSchedulability(e, task1)).collect(Collectors.toSet());
    assertEquals(expectedExecutors1, candidateExecutors1);

    final Set<ExecutorRepresenter> candidateExecutors2 = executorRepresenters.stream()
      .filter(e -> schedulingConstraint.testSchedulability(e, task2)).collect(Collectors.toSet());
    assertEquals(expectedExecutors2, candidateExecutors2);

    final Set<ExecutorRepresenter> candidateExecutors3 = executorRepresenters.stream()
      .filter(e -> schedulingConstraint.testSchedulability(e, task3)).collect(Collectors.toSet());
    assertEquals(expectedExecutors1, candidateExecutors3);
  }
}
