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
package edu.snu.vortex.runtime.master;

import edu.snu.vortex.runtime.common.RuntimeIdGenerator;
import edu.snu.vortex.runtime.common.comm.ControlMessage;
import edu.snu.vortex.runtime.common.message.MessageContext;
import edu.snu.vortex.runtime.common.state.PartitionState;
import edu.snu.vortex.common.StateMachine;
import edu.snu.vortex.runtime.exception.AbsentPartitionException;

import javax.annotation.Nullable;
import javax.annotation.concurrent.ThreadSafe;
import javax.inject.Inject;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.stream.IntStream;

import edu.snu.vortex.runtime.master.metadata.MetadataManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static edu.snu.vortex.runtime.common.state.PartitionState.State.COMMITTED;
import static edu.snu.vortex.runtime.common.state.PartitionState.State.SCHEDULED;

/**
 * Master-side partition manager.
 * For now, all its operations are synchronized to guarantee thread safety.
 * TODO #430: Handle Concurrency at Partition Level.
 * TODO #431: Include Partition Metadata in a Partition.
 * TODO #433: Reconsider fault tolerance for partitions in remote storage.
 */
@ThreadSafe
public final class PartitionManagerMaster {
  private static final Logger LOG = LoggerFactory.getLogger(PartitionManagerMaster.class.getName());
  private static final String REMOTE_FILE_LOCATION = "REMOTE_FILE_STORE";
  private final Map<String, PartitionState> partitionIdToState;
  // Committed partition id to the location (worker id, remote file store, ...)
  private final Map<String, String> committedPartitionIdToLocation;
  private final Map<String, Set<String>> producerTaskGroupIdToPartitionIds;
  private final Map<String, CompletableFuture<String>> partitionIdToLocationFuture;
  // Partition id to the remaining partial committers (task idx).
  private final Map<String, Set<Integer>> partitionIdToRemainingCommitterIdx;
  private final MetadataManager metadataManager;

  @Inject
  private PartitionManagerMaster(final MetadataManager metadataManager) {
    this.partitionIdToState = new HashMap<>();
    this.committedPartitionIdToLocation = new HashMap<>();
    this.producerTaskGroupIdToPartitionIds = new HashMap<>();
    this.partitionIdToLocationFuture = new HashMap<>();
    this.partitionIdToRemainingCommitterIdx = new HashMap<>();
    this.metadataManager = metadataManager;
  }

  public synchronized void initializeState(final String edgeId, final int srcTaskIndex,
                                           final String producerTaskGroupId) {
    final String partitionId = RuntimeIdGenerator.generatePartitionId(edgeId, srcTaskIndex);
    partitionIdToState.put(partitionId, new PartitionState());
    producerTaskGroupIdToPartitionIds.putIfAbsent(producerTaskGroupId, new HashSet<>());
    producerTaskGroupIdToPartitionIds.get(producerTaskGroupId).add(partitionId);
  }

  public synchronized void initializeState(final String edgeId, final int srcTaskIndex, final int partitionIndex,
                                           final String producerTaskGroupId) {
    final String partitionId = RuntimeIdGenerator.generatePartitionId(edgeId, srcTaskIndex, partitionIndex);
    partitionIdToState.put(partitionId, new PartitionState());
    producerTaskGroupIdToPartitionIds.putIfAbsent(producerTaskGroupId, new HashSet<>());
    producerTaskGroupIdToPartitionIds.get(producerTaskGroupId).add(partitionId);
  }

  public synchronized void initializeState(final String edgeId,
                                           final int dstTaskIndex,
                                           final Set<String> producerTaskGroupIds) {
    final String partitionId = RuntimeIdGenerator.generatePartitionId(edgeId, dstTaskIndex);
    partitionIdToState.put(partitionId, new PartitionState());
    producerTaskGroupIds.forEach(producerTaskGroupId -> {
      producerTaskGroupIdToPartitionIds.putIfAbsent(producerTaskGroupId, new HashSet<>());
      producerTaskGroupIdToPartitionIds.get(producerTaskGroupId).add(partitionId);
    });
    final Set<Integer> remainingCommitterIdx = new HashSet<>();
    IntStream.range(0, producerTaskGroupIds.size()).forEach(remainingCommitterIdx::add);
    partitionIdToRemainingCommitterIdx.put(partitionId, remainingCommitterIdx);
  }

  public synchronized Set<String> removeWorker(final String executorId) {
    final Set<String> taskGroupsToRecompute = new HashSet<>();

    // Set committed partition states to lost
    getCommittedPartitionsByWorker(executorId).forEach(partitionId -> {
      onPartitionStateChanged(partitionId, PartitionState.State.LOST, executorId, null);
      // producerTaskGroupForPartition should always be non-empty.
      final Set<String> producerTaskGroupForPartition = getProducerTaskGroupIds(partitionId).get();
      producerTaskGroupForPartition.forEach(taskGroupsToRecompute::add);
    });

    // Update worker-related global variables
    committedPartitionIdToLocation.entrySet().removeIf(e -> e.getValue().equals(executorId));

    return taskGroupsToRecompute;
  }

  public synchronized Optional<String> getPartitionLocation(final String partitionId) {
    final String executorId = committedPartitionIdToLocation.get(partitionId);
    return Optional.ofNullable(executorId);
  }

  /**
   * Return a {@link CompletableFuture} of partition location, which is not yet resolved in {@code SCHEDULED} state.
   * @param partitionId id of the specified partition
   * @return {@link CompletableFuture} of partition location, which completes exceptionally when the partition
   *         is not {@code SCHEDULED} or {@code COMMITTED}.
   */
  public synchronized CompletableFuture<String> getPartitionLocationFuture(final String partitionId) {
    final PartitionState.State state =
        (PartitionState.State) getPartitionState(partitionId).getStateMachine().getCurrentState();
    switch (state) {
      case SCHEDULED:
      case PARTIAL_COMMITTED:
        // TODO #444: Introduce BlockState -> remove PARTIAL_COMMITTED and manage the block state in PartitionStores.
        return partitionIdToLocationFuture.computeIfAbsent(partitionId, pId -> new CompletableFuture<>());
      case COMMITTED:
        return CompletableFuture.completedFuture(getPartitionLocation(partitionId).get());
      case READY:
      case LOST_BEFORE_COMMIT:
      case LOST:
      case REMOVED:
        final CompletableFuture<String> future = new CompletableFuture<>();
        future.completeExceptionally(new AbsentPartitionException(partitionId, state));
        return future;
      default:
        throw new UnsupportedOperationException(state.toString());
    }
  }

  public synchronized Optional<Set<String>> getProducerTaskGroupIds(final String partitionId) {
    final Set<String> producerTaskGroupIds = new HashSet<>();
    for (Map.Entry<String, Set<String>> entry : producerTaskGroupIdToPartitionIds.entrySet()) {
      if (entry.getValue().contains(partitionId)) {
        producerTaskGroupIds.add(entry.getKey());
      }
    }

    if (producerTaskGroupIds.isEmpty()) {
      return Optional.empty();
    } else {
      return Optional.of(producerTaskGroupIds);
    }
  }

  /**
   * To be called when a potential producer task group is scheduled.
   * To be precise, it is called when the task group is enqueued to
   * {@link edu.snu.vortex.runtime.master.scheduler.PendingTaskGroupPriorityQueue}
   * @param scheduledTaskGroupId the ID of the scheduled task group.
   */
  public synchronized void onProducerTaskGroupScheduled(final String scheduledTaskGroupId) {
    if (producerTaskGroupIdToPartitionIds.containsKey(scheduledTaskGroupId)) {
      producerTaskGroupIdToPartitionIds.get(scheduledTaskGroupId).forEach(partitionId -> {
        if (!partitionIdToState.get(partitionId).getStateMachine().getCurrentState().equals(SCHEDULED)) {
          onPartitionStateChanged(partitionId, SCHEDULED, null, null);
        }
      });
    } // else this task group does not produce any partition
  }

  /**
   * To be called when a potential producer task group fails.
   * Only the TaskGroups that have not yet completed (i.e. partitions not yet committed) will call this method.
   * @param failedTaskGroupId the ID of the task group that failed.
   */
  public synchronized void onProducerTaskGroupFailed(final String failedTaskGroupId) {
    if (producerTaskGroupIdToPartitionIds.containsKey(failedTaskGroupId)) {
      producerTaskGroupIdToPartitionIds.get(failedTaskGroupId).forEach(partitionId ->
          onPartitionStateChanged(partitionId, PartitionState.State.LOST_BEFORE_COMMIT, null, null));
    } // else this task group does not produce any partition
  }

  public synchronized Set<String> getCommittedPartitionsByWorker(final String executorId) {
    final Set<String> partitionIds = new HashSet<>();
    committedPartitionIdToLocation.forEach((partitionId, workerId) -> {
      if (workerId.equals(executorId)) {
        partitionIds.add(partitionId);
      }
    });
    return partitionIds;
  }

  public synchronized PartitionState getPartitionState(final String partitionId) {
    return partitionIdToState.get(partitionId);
  }

  public synchronized void onPartitionStateChanged(final String partitionId,
                                                   final PartitionState.State newState,
                                                   @Nullable final String committedWorkerId,
                                                   @Nullable final Integer committedTaskIdx) {
    final StateMachine sm = partitionIdToState.get(partitionId).getStateMachine();
    final Enum oldState = sm.getCurrentState();
    LOG.debug("Partition State Transition: id {} from {} to {}",
        new Object[]{partitionId, oldState, newState});

    sm.setState(newState);

    switch (newState) {
      case SCHEDULED:
        break;
      case LOST_BEFORE_COMMIT:
        completeLocationFuture(partitionId, newState, Optional.empty());
        break;
      case COMMITTED:
        committedPartitionIdToLocation.put(partitionId, committedWorkerId);
        completeLocationFuture(partitionId, newState, Optional.of(committedWorkerId));
        break;
      case PARTIAL_COMMITTED:
        final Set<Integer> remainingCommitters = partitionIdToRemainingCommitterIdx.get(partitionId);
        remainingCommitters.remove(committedTaskIdx);
        if (remainingCommitters.isEmpty()) { // All committers committed their data.
          partitionIdToRemainingCommitterIdx.remove(remainingCommitters);
          onPartitionStateChanged(partitionId, COMMITTED, REMOTE_FILE_LOCATION, null);
        }
        break;
      case REMOVED:
        committedPartitionIdToLocation.remove(partitionId);
        completeLocationFuture(partitionId, newState, Optional.empty());
        break;
      case LOST:
        LOG.info("Partition {} lost in {}", new Object[]{partitionId, committedWorkerId});
        committedPartitionIdToLocation.remove(partitionId);
        completeLocationFuture(partitionId, newState, Optional.empty());
        break;
      default:
        throw new UnsupportedOperationException(newState.toString());
    }
  }

  private synchronized void completeLocationFuture(final String partitionId,
                                                   final PartitionState.State state,
                                                   final Optional<String> result) {
    partitionIdToLocationFuture.entrySet().removeIf(e -> {
      if (e.getKey().equals(partitionId)) {
        if (result.isPresent()) {
          e.getValue().complete(result.get());
        } else {
          e.getValue().completeExceptionally(new AbsentPartitionException(partitionId, state));
        }
        return true;
      }
      return false;
    });
  }

  public synchronized void onRequestPartitionLocation(final ControlMessage.Message message,
                                                      final MessageContext messageContext) {
    final ControlMessage.RequestPartitionLocationMsg requestPartitionLocationMsg =
        message.getRequestPartitionLocationMsg();
    final CompletableFuture<String> locationFuture
        = getPartitionLocationFuture(requestPartitionLocationMsg.getPartitionId());
    locationFuture.whenComplete((location, throwable) -> {
      final ControlMessage.PartitionLocationInfoMsg.Builder infoMsgBuilder =
          ControlMessage.PartitionLocationInfoMsg.newBuilder()
              .setRequestId(message.getId())
              .setPartitionId(requestPartitionLocationMsg.getPartitionId());
      if (throwable == null) {
        infoMsgBuilder.setOwnerExecutorId(location);
      } else {
        infoMsgBuilder.setState(RuntimeMaster.convertPartitionState(((AbsentPartitionException) throwable).getState()));
      }
      messageContext.reply(
          ControlMessage.Message.newBuilder()
              .setId(RuntimeIdGenerator.generateMessageId())
              .setType(ControlMessage.MessageType.PartitionLocationInfo)
              .setPartitionLocationInfoMsg(infoMsgBuilder.build())
              .build());
    });
  }

  /**
   * Gets the metadata manger.
   *
   * @return the metadata manager.
   */
  MetadataManager getMetadataManager() {
    return metadataManager;
  }
}
