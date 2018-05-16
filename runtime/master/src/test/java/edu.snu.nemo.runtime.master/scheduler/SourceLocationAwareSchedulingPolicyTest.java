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
import edu.snu.nemo.common.ir.Readable;
import edu.snu.nemo.runtime.master.resource.ExecutorRepresenter;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.*;

/**
 * Test cases for {@link SourceLocationAwareSchedulingPolicy}.
 */
@RunWith(PowerMockRunner.class)
@PrepareForTest({ExecutorRepresenter.class, ScheduledTaskGroup.class, Readable.class})
public final class SourceLocationAwareSchedulingPolicyTest {
  private static final String SITE_0 = "SEOUL";
  private static final String SITE_1 = "JINJU";
  private static final String SITE_2 = "BUSAN";

  private static ExecutorRepresenter mockExecutorRepresenter(final String executorId) {
    final ExecutorRepresenter executorRepresenter = mock(ExecutorRepresenter.class);
    when(executorRepresenter.getNodeName()).thenReturn(executorId);
    return executorRepresenter;
  }

  /**
   * {@link SourceLocationAwareSchedulingPolicy} should fail to schedule a {@link ScheduledTaskGroup} when
   * there are no executors in appropriate location(s).
   */
  @Test
  public void testSourceLocationAwareSchedulingNotAvailable() {
    final SchedulingPolicy schedulingPolicy = new SourceLocationAwareSchedulingPolicy();

    // Prepare test scenario
    final ScheduledTaskGroup tg = CreateScheduledTaskGroup.withReadablesWithSourceLocations(
        Collections.singletonList(Collections.singletonList(SITE_0)));
    final ExecutorRepresenter e0 = mockExecutorRepresenter(SITE_1);
    final ExecutorRepresenter e1 = mockExecutorRepresenter(SITE_1);

    assertEquals(Collections.emptySet(),
        schedulingPolicy.filterExecutorRepresenters(new HashSet<>(Arrays.asList(e0, e1)), tg));
  }

  /**
   * {@link SourceLocationAwareSchedulingPolicy} should properly schedule TGs with multiple source locations.
   */
  @Test
  public void testSourceLocationAwareSchedulingWithMultiSource() {
    final SchedulingPolicy schedulingPolicy = new SourceLocationAwareSchedulingPolicy();
    // Prepare test scenario
    final ScheduledTaskGroup tg0 = CreateScheduledTaskGroup.withReadablesWithSourceLocations(
        Collections.singletonList(Collections.singletonList(SITE_1)));
    final ScheduledTaskGroup tg1 = CreateScheduledTaskGroup.withReadablesWithSourceLocations(
        Collections.singletonList(Arrays.asList(SITE_0, SITE_1, SITE_2)));
    final ScheduledTaskGroup tg2 = CreateScheduledTaskGroup.withReadablesWithSourceLocations(
        Arrays.asList(Collections.singletonList(SITE_0), Collections.singletonList(SITE_1),
            Arrays.asList(SITE_1, SITE_2)));
    final ScheduledTaskGroup tg3 = CreateScheduledTaskGroup.withReadablesWithSourceLocations(
        Arrays.asList(Collections.singletonList(SITE_1), Collections.singletonList(SITE_0),
            Arrays.asList(SITE_0, SITE_2)));

    final ExecutorRepresenter e = mockExecutorRepresenter(SITE_1);
    for (final ScheduledTaskGroup tg : new HashSet<>(Arrays.asList(tg0, tg1, tg2, tg3))) {
      assertEquals(new HashSet<>(Collections.singletonList(e)), schedulingPolicy.filterExecutorRepresenters(
          new HashSet<>(Collections.singletonList(e)), tg));
    }
  }


  /**
   * Utility for creating {@link ScheduledTaskGroup}.
   */
  private static final class CreateScheduledTaskGroup {
    private static final AtomicInteger taskGroupIndex = new AtomicInteger(0);
    private static final AtomicInteger taskIndex = new AtomicInteger(0);

    private static ScheduledTaskGroup doCreate(final Collection<Readable> readables) {
      final ScheduledTaskGroup mockInstance = mock(ScheduledTaskGroup.class);
      final Map<String, Readable> readableMap = new HashMap<>();
      readables.forEach(readable -> readableMap.put(String.format("TASK-%d", taskIndex.getAndIncrement()),
          readable));
      when(mockInstance.getTaskGroupId()).thenReturn(String.format("TG-%d", taskGroupIndex.getAndIncrement()));
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

    static ScheduledTaskGroup withReadablesWhichThrowException(final int numReadables) {
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

    static ScheduledTaskGroup withoutReadables() {
      return doCreate(Collections.emptyList());
    }
  }
}
