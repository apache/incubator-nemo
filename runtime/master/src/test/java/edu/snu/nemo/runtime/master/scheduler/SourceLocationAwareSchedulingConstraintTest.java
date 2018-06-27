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

import edu.snu.nemo.runtime.common.plan.Task;
import edu.snu.nemo.common.ir.Readable;
import edu.snu.nemo.runtime.master.resource.ExecutorRepresenter;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.*;

/**
 * Test cases for {@link SourceLocationAwareSchedulingConstraint}.
 */
@RunWith(PowerMockRunner.class)
@PrepareForTest({ExecutorRepresenter.class, Task.class, Readable.class})
public final class SourceLocationAwareSchedulingConstraintTest {
  private static final String SITE_0 = "SEOUL";
  private static final String SITE_1 = "JINJU";
  private static final String SITE_2 = "BUSAN";

  private static ExecutorRepresenter mockExecutorRepresenter(final String executorId) {
    final ExecutorRepresenter executorRepresenter = mock(ExecutorRepresenter.class);
    when(executorRepresenter.getNodeName()).thenReturn(executorId);
    return executorRepresenter;
  }

  /**
   * {@link SourceLocationAwareSchedulingConstraint} should fail to schedule a {@link Task} when
   * there are no executors in appropriate location(s).
   */
  @Test
  public void testSourceLocationAwareSchedulingNotAvailable() {
    final SchedulingConstraint schedulingConstraint = new SourceLocationAwareSchedulingConstraint();

    // Prepare test scenario
    final Task task = CreateTask.withReadablesWithSourceLocations(
        Collections.singletonList(Collections.singletonList(SITE_0)));
    final ExecutorRepresenter e0 = mockExecutorRepresenter(SITE_1);
    final ExecutorRepresenter e1 = mockExecutorRepresenter(SITE_1);

    assertEquals(Collections.emptySet(), Arrays.asList(e0, e1).stream()
        .filter(e -> schedulingConstraint.testSchedulability(e, task)).collect(Collectors.toSet()));
  }

  /**
   * {@link SourceLocationAwareSchedulingConstraint} should properly schedule TGs with multiple source locations.
   */
  @Test
  public void testSourceLocationAwareSchedulingWithMultiSource() {
    final SchedulingConstraint schedulingConstraint = new SourceLocationAwareSchedulingConstraint();
    // Prepare test scenario
    final Task task0 = CreateTask.withReadablesWithSourceLocations(
        Collections.singletonList(Collections.singletonList(SITE_1)));
    final Task task1 = CreateTask.withReadablesWithSourceLocations(
        Collections.singletonList(Arrays.asList(SITE_0, SITE_1, SITE_2)));
    final Task task2 = CreateTask.withReadablesWithSourceLocations(
        Arrays.asList(Collections.singletonList(SITE_0), Collections.singletonList(SITE_1),
            Arrays.asList(SITE_1, SITE_2)));
    final Task task3 = CreateTask.withReadablesWithSourceLocations(
        Arrays.asList(Collections.singletonList(SITE_1), Collections.singletonList(SITE_0),
            Arrays.asList(SITE_0, SITE_2)));

    final ExecutorRepresenter e = mockExecutorRepresenter(SITE_1);
    for (final Task task : new HashSet<>(Arrays.asList(task0, task1, task2, task3))) {
      assertTrue(schedulingConstraint.testSchedulability(e, task));
    }
  }


  /**
   * Utility for creating {@link Task}.
   */
  private static final class CreateTask {
    private static final AtomicInteger taskIndex = new AtomicInteger(0);
    private static final AtomicInteger intraTaskIndex = new AtomicInteger(0);

    private static Task doCreate(final Collection<Readable> readables) {
      final Task mockInstance = mock(Task.class);
      final Map<String, Readable> readableMap = new HashMap<>();
      readables.forEach(readable -> readableMap.put(String.format("TASK-%d", intraTaskIndex.getAndIncrement()),
          readable));
      when(mockInstance.getTaskId()).thenReturn(String.format("T-%d", taskIndex.getAndIncrement()));
      when(mockInstance.getIrVertexIdToReadable()).thenReturn(readableMap);
      return mockInstance;
    }

    static Task withReadablesWithSourceLocations(final Collection<List<String>> sourceLocation) {
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

    static Task withReadablesWithoutSourceLocations(final int numReadables) {
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

    static Task withReadablesWhichThrowException(final int numReadables) {
      try {
        final List<Readable> readables = new ArrayList<>();
        for (int i = 0; i < numReadables; i++) {
          final Readable readable = mock(Readable.class);
          when(readable.getLocations()).thenThrow(new UnsupportedOperationException());
          readables.add(readable);
        }
        return doCreate(readables);
      } catch (final Exception e) {
        throw new RuntimeException(e);
      }
    }

    static Task withoutReadables() {
      return doCreate(Collections.emptyList());
    }
  }
}
