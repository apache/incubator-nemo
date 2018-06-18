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
package edu.snu.nemo.runtime.master;

import edu.snu.nemo.runtime.common.RuntimeIdGenerator;
import edu.snu.nemo.runtime.common.exception.AbsentBlockException;
import edu.snu.nemo.runtime.common.message.MessageEnvironment;
import edu.snu.nemo.runtime.common.message.local.LocalMessageDispatcher;
import edu.snu.nemo.runtime.common.message.local.LocalMessageEnvironment;
import edu.snu.nemo.runtime.common.state.BlockState;
import org.apache.reef.tang.Injector;
import org.apache.reef.tang.Tang;
import org.junit.Before;
import org.junit.Test;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
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

  private static void checkBlockAbsentException(final Future<String> future,
                                                final String expectedPartitionId,
                                                final BlockState.State expectedState)
      throws IllegalStateException, InterruptedException {
    assertTrue(future.isDone());
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

  private static void checkBlockLocation(final Future<String> future,
                                         final String expectedLocation)
      throws InterruptedException, ExecutionException {
    assertTrue(future.isDone());
    assertEquals(expectedLocation, future.get()); // must not throw any exception.
  }

  private static void checkPendingFuture(final Future<String> future) {
    assertFalse(future.isDone());
  }

  /**
   * Test scenario where block becomes committed and then lost.
   * @throws Exception
   */
  @Test
  public void testLostAfterCommit() throws Exception {
    final String edgeId = RuntimeIdGenerator.generateStageEdgeId("Edge-0");
    final int srcTaskIndex = 0;
    final String taskId = RuntimeIdGenerator.generateTaskId(srcTaskIndex, "Stage-test");
    final String executorId = RuntimeIdGenerator.generateExecutorId();
    final String blockId = RuntimeIdGenerator.generateBlockId(edgeId, srcTaskIndex);

    // Initially the block state is NOT_AVAILABLE.
    blockManagerMaster.initializeState(blockId, taskId);
    checkBlockAbsentException(blockManagerMaster.getBlockLocationHandler(blockId).getLocationFuture(), blockId,
        BlockState.State.NOT_AVAILABLE);

    // The block is being IN_PROGRESS.
    blockManagerMaster.onProducerTaskScheduled(taskId);
    final Future<String> future = blockManagerMaster.getBlockLocationHandler(blockId).getLocationFuture();
    checkPendingFuture(future);

    // The block is AVAILABLE
    blockManagerMaster.onBlockStateChanged(blockId, BlockState.State.AVAILABLE, executorId);
    checkBlockLocation(future, executorId); // A future, previously pending on IN_PROGRESS state, is now resolved.
    checkBlockLocation(blockManagerMaster.getBlockLocationHandler(blockId).getLocationFuture(), executorId);

    // We lost the block.
    blockManagerMaster.removeWorker(executorId);
    checkBlockAbsentException(blockManagerMaster.getBlockLocationHandler(blockId).getLocationFuture(), blockId,
        BlockState.State.NOT_AVAILABLE);
  }

  /**
   * Test scenario where producer task fails.
   * @throws Exception
   */
  @Test
  public void testBeforeAfterCommit() throws Exception {
    final String edgeId = RuntimeIdGenerator.generateStageEdgeId("Edge-1");
    final int srcTaskIndex = 0;
    final String taskId = RuntimeIdGenerator.generateTaskId(srcTaskIndex, "Stage-Test");
    final String executorId = RuntimeIdGenerator.generateExecutorId();
    final String blockId = RuntimeIdGenerator.generateBlockId(edgeId, srcTaskIndex);

    // The block is being scheduled.
    blockManagerMaster.initializeState(blockId, taskId);
    blockManagerMaster.onProducerTaskScheduled(taskId);
    final Future<String> future0 = blockManagerMaster.getBlockLocationHandler(blockId).getLocationFuture();
    checkPendingFuture(future0);

    // Producer task fails.
    blockManagerMaster.onProducerTaskFailed(taskId);

    // A future, previously pending on IN_PROGRESS state, is now completed exceptionally.
    checkBlockAbsentException(future0, blockId, BlockState.State.NOT_AVAILABLE);
    checkBlockAbsentException(blockManagerMaster.getBlockLocationHandler(blockId).getLocationFuture(), blockId,
        BlockState.State.NOT_AVAILABLE);

    // Re-scheduling the task.
    blockManagerMaster.onProducerTaskScheduled(taskId);
    final Future<String> future1 = blockManagerMaster.getBlockLocationHandler(blockId).getLocationFuture();
    checkPendingFuture(future1);

    // Committed.
    blockManagerMaster.onBlockStateChanged(blockId, BlockState.State.AVAILABLE, executorId);
    checkBlockLocation(future1, executorId); // A future, previously pending on IN_PROGRESS state, is now resolved.
    checkBlockLocation(blockManagerMaster.getBlockLocationHandler(blockId).getLocationFuture(), executorId);

    // Then removed.
    blockManagerMaster.onBlockStateChanged(blockId, BlockState.State.NOT_AVAILABLE, executorId);
    checkBlockAbsentException(blockManagerMaster.getBlockLocationHandler(blockId).getLocationFuture(), blockId,
        BlockState.State.NOT_AVAILABLE);
  }
}
