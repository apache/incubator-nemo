/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.nemo.runtime.executor.datatransfer;

import org.apache.nemo.common.HashRange;
import org.apache.nemo.common.KeyRange;
import org.apache.nemo.common.Pair;
import org.apache.nemo.common.exception.BlockFetchException;
import org.apache.nemo.common.exception.UnsupportedCommPatternException;
import org.apache.nemo.common.ir.edge.executionproperty.*;
import org.apache.nemo.common.ir.vertex.IRVertex;
import org.apache.nemo.runtime.common.RuntimeIdManager;
import org.apache.nemo.common.ir.edge.RuntimeEdge;
import org.apache.nemo.common.ir.edge.StageEdge;
import org.apache.nemo.runtime.executor.data.BlockManagerWorker;
import org.apache.nemo.runtime.executor.data.DataUtil;
import org.apache.reef.wake.EventHandler;

import java.util.*;
import java.util.concurrent.CompletableFuture;

/**
 * Represents the input data transfer to a task.
 */
public final class BlockInputReader implements InputReader {
  private final BlockManagerWorker blockManagerWorker;

  private final int dstTaskIndex;

  /**
   * Attributes that specify how we should read the input.
   */
  private final IRVertex srcVertex;
  private final RuntimeEdge runtimeEdge;

  BlockInputReader(final int dstTaskIndex,
                   final IRVertex srcVertex,
                   final RuntimeEdge runtimeEdge,
                   final BlockManagerWorker blockManagerWorker) {
    this.dstTaskIndex = dstTaskIndex;
    this.srcVertex = srcVertex;
    this.runtimeEdge = runtimeEdge;
    this.blockManagerWorker = blockManagerWorker;
  }

  @Override
  public List<CompletableFuture<DataUtil.IteratorWithNumBytes>> read() {
    final Optional<CommunicationPatternProperty.Value> comValue =
      runtimeEdge.getPropertyValue(CommunicationPatternProperty.class);

    if (comValue.get().equals(CommunicationPatternProperty.Value.OneToOne)) {
      return Collections.singletonList(readOneToOne());
    } else if (comValue.get().equals(CommunicationPatternProperty.Value.BroadCast)) {
      return readBroadcast();
    } else if (comValue.get().equals(CommunicationPatternProperty.Value.Shuffle)) {
      return readDataInRange();
    } else {
      throw new UnsupportedCommPatternException(new Exception("Communication pattern not supported"));
    }
  }

  @Override
  public void readAsync(EventHandler<Pair<DataUtil.IteratorWithNumBytes, Integer>> handler) {
    throw new UnsupportedOperationException("Not support");
  }

  @Override
  public List<DataUtil.IteratorWithNumBytes> readBlocking() {
    throw new UnsupportedOperationException("Not support");
  }

  @Override
  public IRVertex getSrcIrVertex() {
    return srcVertex;
  }

  @Override
  public int getTaskIndex() {
    return dstTaskIndex;
  }

  /**
   * See {@link RuntimeIdManager#generateBlockIdWildcard(String, int)} for information on block wildcards.
   * @param producerTaskIndex to use.
   * @return wildcard block id that corresponds to "ANY" task attempt of the task index.
   */
  private String generateWildCardBlockId(final int producerTaskIndex) {
    final Optional<DuplicateEdgeGroupPropertyValue> duplicateDataProperty =
      runtimeEdge.getPropertyValue(DuplicateEdgeGroupProperty.class);
    if (!duplicateDataProperty.isPresent() || duplicateDataProperty.get().getGroupSize() <= 1) {
      return RuntimeIdManager.generateBlockIdWildcard(runtimeEdge.getId(), producerTaskIndex);
    }
    final String duplicateEdgeId = duplicateDataProperty.get().getRepresentativeEdgeId();
    return RuntimeIdManager.generateBlockIdWildcard(duplicateEdgeId, producerTaskIndex);
  }

  private CompletableFuture<DataUtil.IteratorWithNumBytes> readOneToOne() {
    final String blockIdWildcard = generateWildCardBlockId(dstTaskIndex);
    final Optional<DataStoreProperty.Value> dataStoreProperty
      = runtimeEdge.getPropertyValue(DataStoreProperty.class);
    return blockManagerWorker.readBlock(blockIdWildcard, runtimeEdge.getId(), dataStoreProperty.get(), HashRange.all());
  }

  private List<CompletableFuture<DataUtil.IteratorWithNumBytes>> readBroadcast() {
    final int numSrcTasks = InputReader.getSourceParallelism(this);
    final Optional<DataStoreProperty.Value> dataStoreProperty
      = runtimeEdge.getPropertyValue(DataStoreProperty.class);

    final List<CompletableFuture<DataUtil.IteratorWithNumBytes>> futures = new ArrayList<>();
    for (int srcTaskIdx = 0; srcTaskIdx < numSrcTasks; srcTaskIdx++) {
      final String blockIdWildcard = generateWildCardBlockId(srcTaskIdx);
      futures.add(blockManagerWorker.readBlock(
        blockIdWildcard, runtimeEdge.getId(), dataStoreProperty.get(), HashRange.all()));
    }

    return futures;
  }

  /**
   * Read data in the assigned range of hash value.
   *
   * @return the list of the completable future of the data.
   */
  private List<CompletableFuture<DataUtil.IteratorWithNumBytes>> readDataInRange() {
    assert (runtimeEdge instanceof StageEdge);
    final Optional<DataStoreProperty.Value> dataStoreProperty = runtimeEdge.getPropertyValue(DataStoreProperty.class);
    final List<KeyRange> keyRangeList = ((StageEdge) runtimeEdge).getKeyRanges();
    final KeyRange hashRangeToRead = keyRangeList.get(dstTaskIndex);
    if (hashRangeToRead == null) {
      throw new BlockFetchException(
        new Throwable("The hash range to read is not assigned to " + dstTaskIndex + "'th task"));
    }
    final int numSrcTasks = InputReader.getSourceParallelism(this);
    final List<CompletableFuture<DataUtil.IteratorWithNumBytes>> futures = new ArrayList<>();
    for (int srcTaskIdx = 0; srcTaskIdx < numSrcTasks; srcTaskIdx++) {
      final String blockIdWildcard = generateWildCardBlockId(srcTaskIdx);
      futures.add(
        blockManagerWorker.readBlock(blockIdWildcard, runtimeEdge.getId(), dataStoreProperty.get(), hashRangeToRead));
    }

    return futures;
  }
}
