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
package edu.snu.onyx.runtime.executor.datatransfer;

import com.google.common.annotations.VisibleForTesting;
import edu.snu.onyx.common.ir.edge.executionproperty.DataCommunicationPatternProperty;
import edu.snu.onyx.common.ir.edge.executionproperty.DataStoreProperty;
import edu.snu.onyx.common.ir.vertex.IRVertex;
import edu.snu.onyx.common.ir.executionproperty.ExecutionProperty;
import edu.snu.onyx.runtime.common.RuntimeIdGenerator;
import edu.snu.onyx.runtime.common.data.KeyRange;
import edu.snu.onyx.runtime.common.plan.RuntimeEdge;
import edu.snu.onyx.runtime.common.plan.physical.PhysicalStageEdge;
import edu.snu.onyx.runtime.common.plan.physical.Task;
import edu.snu.onyx.common.exception.BlockFetchException;
import edu.snu.onyx.common.exception.UnsupportedCommPatternException;
import edu.snu.onyx.runtime.common.data.HashRange;
import edu.snu.onyx.runtime.executor.data.BlockManagerWorker;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

/**
 * Represents the input data transfer to a task.
 * TODO #492: Modularize the data communication pattern.
 */
public final class InputReader extends DataTransfer {
  private final int dstTaskIndex;
  private final String taskGroupId;
  private final BlockManagerWorker blockManagerWorker;

  /**
   * Attributes that specify how we should read the input.
   */
  @Nullable
  private final IRVertex srcVertex;
  private final RuntimeEdge runtimeEdge;

  public InputReader(final int dstTaskIndex,
                     final String taskGroupId,
                     final IRVertex srcVertex,
                     final RuntimeEdge runtimeEdge,
                     final BlockManagerWorker blockManagerWorker) {
    super(runtimeEdge.getId());
    this.dstTaskIndex = dstTaskIndex;
    this.taskGroupId = taskGroupId;
    this.srcVertex = srcVertex;
    this.runtimeEdge = runtimeEdge;
    this.blockManagerWorker = blockManagerWorker;
  }

  /**
   * Reads input data depending on the communication pattern of the srcVertex.
   *
   * @return the read data.
   */
  public List<CompletableFuture<Iterable>> read() {
    DataCommunicationPatternProperty.Value comValue =
        (DataCommunicationPatternProperty.Value)
            runtimeEdge.getProperty(ExecutionProperty.Key.DataCommunicationPattern);

    if (comValue.equals(DataCommunicationPatternProperty.Value.OneToOne)) {
      return Collections.singletonList(readOneToOne());
    } else if (comValue.equals(DataCommunicationPatternProperty.Value.BroadCast)) {
      return readBroadcast();
    } else if (comValue.equals(DataCommunicationPatternProperty.Value.Shuffle)) {
      // If the dynamic optimization which detects data skew is enabled, read the data in the assigned range.
      // TODO #492: Modularize the data communication pattern.
      return readDataInRange();
    } else {
      throw new UnsupportedCommPatternException(new Exception("Communication pattern not supported"));
    }
  }

  private CompletableFuture<Iterable> readOneToOne() {
    final String blockId = RuntimeIdGenerator.generateBlockId(getId(), dstTaskIndex);
    return blockManagerWorker.retrieveDataFromBlock(blockId, getId(),
        (DataStoreProperty.Value) runtimeEdge.getProperty(ExecutionProperty.Key.DataStore),
        HashRange.all());
  }

  private List<CompletableFuture<Iterable>> readBroadcast() {
    final int numSrcTasks = this.getSourceParallelism();

    final List<CompletableFuture<Iterable>> futures = new ArrayList<>();
    for (int srcTaskIdx = 0; srcTaskIdx < numSrcTasks; srcTaskIdx++) {
      final String blockId = RuntimeIdGenerator.generateBlockId(getId(), srcTaskIdx);
      futures.add(blockManagerWorker.retrieveDataFromBlock(blockId, getId(),
          (DataStoreProperty.Value) runtimeEdge.getProperty(ExecutionProperty.Key.DataStore),
          HashRange.all()));
    }

    return futures;
  }

  /**
   * Read data in the assigned range of hash value.
   * Constraint: If a block is written by {@link OutputWriter#dataSkewWrite(List)}
   * or {@link OutputWriter#writeShuffle(List)}, it must be read using this method.
   *
   * @return the list of the completable future of the data.
   */
  private List<CompletableFuture<Iterable>> readDataInRange() {
    assert (runtimeEdge instanceof PhysicalStageEdge);
    final KeyRange hashRangeToRead =
        ((PhysicalStageEdge) runtimeEdge).getTaskGroupIdToKeyRangeMap().get(taskGroupId);
    if (hashRangeToRead == null) {
      throw new BlockFetchException(new Throwable("The hash range to read is not assigned to " + taskGroupId));
    }

    final int numSrcTasks = this.getSourceParallelism();
    final List<CompletableFuture<Iterable>> futures = new ArrayList<>();
    for (int srcTaskIdx = 0; srcTaskIdx < numSrcTasks; srcTaskIdx++) {
      final String blockId = RuntimeIdGenerator.generateBlockId(getId(), srcTaskIdx);
      futures.add(
          blockManagerWorker.retrieveDataFromBlock(blockId, getId(),
              (DataStoreProperty.Value) runtimeEdge.getProperty(ExecutionProperty.Key.DataStore),
              hashRangeToRead));
    }

    return futures;
  }

  public RuntimeEdge getRuntimeEdge() {
    return runtimeEdge;
  }

  public String getSrcVertexId() {
    // this src vertex can be either a real vertex or a task. we must check!
    if (srcVertex != null) {
      return srcVertex.getId();
    }

    return ((Task) runtimeEdge.getSrc()).getRuntimeVertexId();
  }

  public boolean isSideInputReader() {
    return Boolean.TRUE.equals(runtimeEdge.isSideInput());
  }

  public CompletableFuture<Object> getSideInput() {
    if (!isSideInputReader()) {
      throw new RuntimeException();
    }
    final CompletableFuture<Iterable> future = this.read().get(0);
    return future.thenApply(f -> f.iterator().next());
  }

  /**
   * Get the parallelism of the source task.
   *
   * @return the parallelism of the source task.
   */
  public int getSourceParallelism() {
    if (srcVertex != null) {
      if (DataCommunicationPatternProperty.Value.OneToOne
          .equals(runtimeEdge.getProperty(ExecutionProperty.Key.DataCommunicationPattern))) {
        return 1;
      } else {
        final Integer numSrcTasks = srcVertex.getProperty(ExecutionProperty.Key.Parallelism);
        return numSrcTasks;
      }
    } else {
      // Memory input reader
      return 1;
    }
  }

  /**
   * Combine the given list of futures.
   *
   * @param futures to combine.
   * @return the combined iterable of elements.
   * @throws ExecutionException   when fail to get results from futures.
   * @throws InterruptedException when interrupted during getting results from futures.
   */
  @VisibleForTesting
  public static Iterable combineFutures(final List<CompletableFuture<Iterable>> futures)
      throws ExecutionException, InterruptedException {
    final List concatStreamBase = new ArrayList<>();
    Stream<Object> concatStream = concatStreamBase.stream();
    for (int srcTaskIdx = 0; srcTaskIdx < futures.size(); srcTaskIdx++) {
      final Iterable dataFromATask = futures.get(srcTaskIdx).get();
      concatStream = Stream.concat(concatStream, StreamSupport.stream(dataFromATask.spliterator(), false));
    }
    return concatStream.collect(Collectors.toList());
  }
}
