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
package edu.snu.nemo.runtime.master.scheduler;

import edu.snu.nemo.runtime.common.plan.physical.ScheduledTaskGroup;
import edu.snu.nemo.runtime.master.resource.ExecutorRepresenter;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.util.*;
import java.util.stream.IntStream;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.*;

/**
 * Tests {@link RoundRobinSchedulingPolicy}
 */
@RunWith(PowerMockRunner.class)
@PrepareForTest({ExecutorRepresenter.class, ScheduledTaskGroup.class})
public final class RoundRobinSchedulingPolicyTest {

  private static ExecutorRepresenter mockExecutorRepresenter(final int numRunningTaskGroups) {
    final ExecutorRepresenter executorRepresenter = mock(ExecutorRepresenter.class);
    final Set<String> runningTaskGroups = new HashSet<>();
    IntStream.range(0, numRunningTaskGroups).forEach(i -> runningTaskGroups.add(String.valueOf(i)));
    when(executorRepresenter.getRunningTaskGroups()).thenReturn(runningTaskGroups);
    return executorRepresenter;
  }

  @Test
  public void testRoundRobin() {
    final SchedulingPolicy schedulingPolicy = new RoundRobinSchedulingPolicy();
    final ExecutorRepresenter a0 = mockExecutorRepresenter(1);
    final ExecutorRepresenter a1 = mockExecutorRepresenter(2);
    final ExecutorRepresenter a2 = mockExecutorRepresenter(2);

    final ScheduledTaskGroup scheduledTaskGroup = mock(ScheduledTaskGroup.class);

    final Set<ExecutorRepresenter> executorRepresenterList = new HashSet<>(Arrays.asList(a0, a1, a2));

    final Set<ExecutorRepresenter> candidateExecutors =
        schedulingPolicy.filterExecutorRepresenters(executorRepresenterList, scheduledTaskGroup);

    final Set<ExecutorRepresenter> expectedExecutors = new HashSet<>(Arrays.asList(a0));
    assertEquals(expectedExecutors, candidateExecutors);
  }
}

