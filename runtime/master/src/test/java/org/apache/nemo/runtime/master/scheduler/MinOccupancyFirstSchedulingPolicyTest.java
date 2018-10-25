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
package org.apache.nemo.runtime.master.scheduler;

import org.apache.nemo.runtime.common.plan.Task;
import org.apache.nemo.runtime.master.resource.ExecutorRepresenter;
import org.apache.reef.tang.Tang;
import org.apache.reef.tang.exceptions.InjectionException;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.util.*;
import java.util.stream.IntStream;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.*;

/**
 * Tests {@link MinOccupancyFirstSchedulingPolicy}
 */
@RunWith(PowerMockRunner.class)
@PrepareForTest({ExecutorRepresenter.class, Task.class})
public final class MinOccupancyFirstSchedulingPolicyTest {

  private static Task mockTask(final String taskId) {
    final Task task = mock(Task.class);
    when(task.getTaskId()).thenReturn(taskId);
    return task;
  }

  private static ExecutorRepresenter mockExecutorRepresenter(final int numRunningTasks) {
    final ExecutorRepresenter executorRepresenter = mock(ExecutorRepresenter.class);
    final Set<Task> runningTasks = new HashSet<>();
    IntStream.range(0, numRunningTasks).forEach(i -> runningTasks.add(mockTask(String.valueOf(i))));
    when(executorRepresenter.getRunningTasks()).thenReturn(runningTasks);
    return executorRepresenter;
  }

  @Test
  public void test() throws InjectionException {
    final SchedulingPolicy schedulingPolicy = Tang.Factory.getTang().newInjector()
        .getInstance(MinOccupancyFirstSchedulingPolicy.class);
    final ExecutorRepresenter a0 = mockExecutorRepresenter(1);
    final ExecutorRepresenter a1 = mockExecutorRepresenter(2);
    final ExecutorRepresenter a2 = mockExecutorRepresenter(2);

    final Task task = mock(Task.class);

    final List<ExecutorRepresenter> executorRepresenterList = Arrays.asList(a0, a1, a2);

    assertEquals(a0, schedulingPolicy.selectExecutor(executorRepresenterList, task));
  }
}

