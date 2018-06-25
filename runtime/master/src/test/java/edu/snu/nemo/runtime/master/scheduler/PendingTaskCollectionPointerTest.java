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
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Optional;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;

/**
 * Tests {@link PendingTaskCollectionPointer}
 */
@RunWith(PowerMockRunner.class)
@PrepareForTest({Task.class})
public final class PendingTaskCollectionPointerTest {
  private PendingTaskCollectionPointer pendingTaskCollectionPointer;

  private List<Task> mockTaskList() {
    final Task task = mock(Task.class);
    return Arrays.asList(task);
  }

  @Before
  public void setUp() {
    this.pendingTaskCollectionPointer = new PendingTaskCollectionPointer();
  }

  @Test
  public void nullByDefault() {
    assertFalse(pendingTaskCollectionPointer.getAndSetNull().isPresent());
  }

  @Test
  public void setIfNull() {
    final List<Task> taskList = mockTaskList();
    pendingTaskCollectionPointer.setIfNull(taskList);
    final Optional<Collection<Task>> optional = pendingTaskCollectionPointer.getAndSetNull();
    assertTrue(optional.isPresent());
    assertEquals(taskList, optional.get());
  }

  @Test
  public void setToOverwrite() {
    final List<Task> taskList1 = mockTaskList();
    pendingTaskCollectionPointer.setIfNull(taskList1);
    final List<Task> taskList2 = mockTaskList();
    pendingTaskCollectionPointer.setToOverwrite(taskList2);
    final Optional<Collection<Task>> optional = pendingTaskCollectionPointer.getAndSetNull();
    assertTrue(optional.isPresent());
    assertEquals(taskList2, optional.get());
  }
}

