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
package edu.snu.coral.tests.runtime.master;

import edu.snu.coral.runtime.common.RuntimeIdGenerator;
import edu.snu.coral.runtime.common.exception.AbsentBlockException;
import edu.snu.coral.runtime.common.message.MessageEnvironment;
import edu.snu.coral.runtime.common.message.local.LocalMessageDispatcher;
import edu.snu.coral.runtime.common.message.local.LocalMessageEnvironment;
import edu.snu.coral.runtime.common.state.BlockState;
import edu.snu.coral.runtime.master.BlockManagerMaster;
import org.apache.reef.tang.Injector;
import org.apache.reef.tang.Tang;
import org.junit.Before;
import org.junit.Test;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * Test for {@link BlockManagerMaster}.
 */
public final class BlockManagerMasterTest {
  private BlockManagerMaster blockManagerMaster;

  @Before
  public void setUp() throws Exception {
    final LocalMessageDispatcher messageDispatcher = new LocalMessageDispatcher();
    final LocalMessageEnvironment messageEnvironment =
        new LocalMessageEnvironment(MessageEnvironment.MASTER_COMMUNICATION_ID, messageDispatcher);
    final Injector injector = Tang.Factory.getTang().newInjector();
    injector.bindVolatileInstance(MessageEnvironment.class, messageEnvironment);
    blockManagerMaster = injector.getInstance(BlockManagerMaster.class);
  }

  private static void checkBlockAbsentException(final CompletableFuture<String> future,
                                                final String expectedPartitionId,
                                                final BlockState.State expectedState)
      throws IllegalStateException, InterruptedException {
    assertTrue(future.isCompletedExceptionally());
    try {
      future.get();
      throw new IllegalStateException("An ExecutionException was expected.");
    } catch (final ExecutionException executionException) {
      final AbsentBlockException absentBlockException
          = (AbsentBlockException) executionException.getCause();
      assertEquals(expectedPartitionId, absentBlockException.getBlockId());
      assertEquals(expectedState, absentBlockException.getState());
    }
  }

  private static void checkBlockLocation(final CompletableFuture<String> future,
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
   * Test scenario where block becomes committed and then lost.
   * @throws Exception
   */
  @Test
  public void testLostAfterCommit() throws Exception {
    final String edgeId = RuntimeIdGenerator.generateRuntimeEdgeId("Edge-0");
    final int srcTaskIndex = 0;
    final String taskGroupId = RuntimeIdGenerator.generateTaskGroupId(srcTaskIndex, "Stage-test");
    final String executorId = RuntimeIdGenerator.generateExecutorId();
    final String blockId = RuntimeIdGenerator.generateBlockId(edgeId, srcTaskIndex);

    // Initially the block state is READY.
    blockManagerMaster.initializeState(blockId, taskGroupId);
    checkBlockAbsentException(blockManagerMaster.getBlockLocationFuture(blockId), blockId,
        BlockState.State.READY);

    // The block is being SCHEDULED.
    blockManagerMaster.onProducerTaskGroupScheduled(taskGroupId);
    final CompletableFuture<String> future = blockManagerMaster.getBlockLocationFuture(blockId);
    checkPendingFuture(future);

    // The block is COMMITTED
    blockManagerMaster.onBlockStateChanged(blockId, BlockState.State.COMMITTED, executorId);
    checkBlockLocation(future, executorId); // A future, previously pending on SCHEDULED state, is now resolved.
    checkBlockLocation(blockManagerMaster.getBlockLocationFuture(blockId), executorId);

    // We LOST the block.
    blockManagerMaster.removeWorker(executorId);
    checkBlockAbsentException(blockManagerMaster.getBlockLocationFuture(blockId), blockId,
        BlockState.State.LOST);
  }

  /**
   * Test scenario where producer task group fails.
   * @throws Exception
   */
  @Test
  public void testBeforeAfterCommit() throws Exception {
    final String edgeId = RuntimeIdGenerator.generateRuntimeEdgeId("Edge-1");
    final int srcTaskIndex = 0;
    final String taskGroupId = RuntimeIdGenerator.generateTaskGroupId(srcTaskIndex, "Stage-Test");
    final String executorId = RuntimeIdGenerator.generateExecutorId();
    final String blockId = RuntimeIdGenerator.generateBlockId(edgeId, srcTaskIndex);

    // The block is being scheduled.
    blockManagerMaster.initializeState(blockId, taskGroupId);
    blockManagerMaster.onProducerTaskGroupScheduled(taskGroupId);
    final CompletableFuture<String> future0 = blockManagerMaster.getBlockLocationFuture(blockId);
    checkPendingFuture(future0);

    // Producer task group fails.
    blockManagerMaster.onProducerTaskGroupFailed(taskGroupId);

    // A future, previously pending on SCHEDULED state, is now completed exceptionally.
    checkBlockAbsentException(future0, blockId, BlockState.State.LOST_BEFORE_COMMIT);
    checkBlockAbsentException(blockManagerMaster.getBlockLocationFuture(blockId), blockId,
        BlockState.State.LOST_BEFORE_COMMIT);

    // Re-scheduling the taskGroup.
    blockManagerMaster.onProducerTaskGroupScheduled(taskGroupId);
    final CompletableFuture<String> future1 = blockManagerMaster.getBlockLocationFuture(blockId);
    checkPendingFuture(future1);

    // Committed.
    blockManagerMaster.onBlockStateChanged(blockId, BlockState.State.COMMITTED, executorId);
    checkBlockLocation(future1, executorId); // A future, previously pending on SCHEDULED state, is now resolved.
    checkBlockLocation(blockManagerMaster.getBlockLocationFuture(blockId), executorId);

    // Then removed.
    blockManagerMaster.onBlockStateChanged(blockId, BlockState.State.REMOVED, executorId);
    checkBlockAbsentException(blockManagerMaster.getBlockLocationFuture(blockId), blockId,
        BlockState.State.REMOVED);
  }
}
