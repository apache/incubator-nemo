/*
 * Copyright (C) 2017 Seoul National University
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
package edu.snu.onyx.runtime.master;

import edu.snu.onyx.runtime.common.RuntimeIdGenerator;
import edu.snu.onyx.runtime.common.state.PartitionState;
import edu.snu.onyx.runtime.exception.AbsentPartitionException;
import org.junit.Before;
import org.junit.Test;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * Test for {@link PartitionManagerMaster}.
 */
public final class PartitionManagerMasterTest {
  private PartitionManagerMaster partitionManagerMaster;

  @Before
  public void setUp() throws Exception {
    partitionManagerMaster = new PartitionManagerMaster();
  }

  private static void checkPartitionAbsentException(final CompletableFuture<String> future,
                                                    final String expectedPartitionId,
                                                    final PartitionState.State expectedState)
      throws IllegalStateException, InterruptedException {
    assertTrue(future.isCompletedExceptionally());
    try {
      future.get();
      throw new IllegalStateException("An ExecutionException was expected.");
    } catch (final ExecutionException executionException) {
      final AbsentPartitionException absentPartitionException
          = (AbsentPartitionException) executionException.getCause();
      assertEquals(expectedPartitionId, absentPartitionException.getPartitionId());
      assertEquals(expectedState, absentPartitionException.getState());
    }
  }

  private static void checkPartitionLocation(final CompletableFuture<String> future,
                                             final String expectedLocation)
      throws InterruptedException, ExecutionException {
    assertTrue(future.isDone());
    assertFalse(future.isCompletedExceptionally());
    assertEquals(expectedLocation, future.get());
  }

  private static void checkPendingFuture(final CompletableFuture<String> future) {
    assertFalse(future.isDone());
  }

  /**
   * Test scenario where partition becomes committed and then lost.
   * @throws Exception
   */
  @Test
  public void testLostAfterCommit() throws Exception {
    final String edgeId = RuntimeIdGenerator.generateRuntimeEdgeId("Edge-0");
    final int srcTaskIndex = 0;
    final String taskGroupId = RuntimeIdGenerator.generateTaskGroupId();
    final String executorId = RuntimeIdGenerator.generateExecutorId();
    final String partitionId = RuntimeIdGenerator.generatePartitionId(edgeId, srcTaskIndex);

    // Initially the partition state is READY.
    partitionManagerMaster.initializeState(partitionId, taskGroupId);
    checkPartitionAbsentException(partitionManagerMaster.getPartitionLocationFuture(partitionId), partitionId,
        PartitionState.State.READY);

    // The partition is being SCHEDULED.
    partitionManagerMaster.onProducerTaskGroupScheduled(taskGroupId);
    final CompletableFuture<String> future = partitionManagerMaster.getPartitionLocationFuture(partitionId);
    checkPendingFuture(future);

    // The partition is COMMITTED
    partitionManagerMaster.onPartitionStateChanged(partitionId, PartitionState.State.COMMITTED, executorId);
    checkPartitionLocation(future, executorId); // A future, previously pending on SCHEDULED state, is now resolved.
    checkPartitionLocation(partitionManagerMaster.getPartitionLocationFuture(partitionId), executorId);

    // We LOST the partition.
    partitionManagerMaster.removeWorker(executorId);
    checkPartitionAbsentException(partitionManagerMaster.getPartitionLocationFuture(partitionId), partitionId,
        PartitionState.State.LOST);
  }

  /**
   * Test scenario where producer task group fails.
   * @throws Exception
   */
  @Test
  public void testBeforeAfterCommit() throws Exception {
    final String edgeId = RuntimeIdGenerator.generateRuntimeEdgeId("Edge-1");
    final int srcTaskIndex = 0;
    final String taskGroupId = RuntimeIdGenerator.generateTaskGroupId();
    final String executorId = RuntimeIdGenerator.generateExecutorId();
    final String partitionId = RuntimeIdGenerator.generatePartitionId(edgeId, srcTaskIndex);

    // The partition is being scheduled.
    partitionManagerMaster.initializeState(partitionId, taskGroupId);
    partitionManagerMaster.onProducerTaskGroupScheduled(taskGroupId);
    final CompletableFuture<String> future0 = partitionManagerMaster.getPartitionLocationFuture(partitionId);
    checkPendingFuture(future0);

    // Producer task group fails.
    partitionManagerMaster.onProducerTaskGroupFailed(taskGroupId);

    // A future, previously pending on SCHEDULED state, is now completed exceptionally.
    checkPartitionAbsentException(future0, partitionId, PartitionState.State.LOST_BEFORE_COMMIT);
    checkPartitionAbsentException(partitionManagerMaster.getPartitionLocationFuture(partitionId), partitionId,
        PartitionState.State.LOST_BEFORE_COMMIT);

    // Re-scheduling the taskGroup.
    partitionManagerMaster.onProducerTaskGroupScheduled(taskGroupId);
    final CompletableFuture<String> future1 = partitionManagerMaster.getPartitionLocationFuture(partitionId);
    checkPendingFuture(future1);

    // Committed.
    partitionManagerMaster.onPartitionStateChanged(partitionId, PartitionState.State.COMMITTED, executorId);
    checkPartitionLocation(future1, executorId); // A future, previously pending on SCHEDULED state, is now resolved.
    checkPartitionLocation(partitionManagerMaster.getPartitionLocationFuture(partitionId), executorId);

    // Then removed.
    partitionManagerMaster.onPartitionStateChanged(partitionId, PartitionState.State.REMOVED, executorId);
    checkPartitionAbsentException(partitionManagerMaster.getPartitionLocationFuture(partitionId), partitionId,
        PartitionState.State.REMOVED);
  }
}
