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
import org.apache.nemo.common.exception.BlockFetchException;
import org.apache.nemo.common.exception.UnsupportedCommPatternException;
import org.apache.nemo.common.ir.edge.executionproperty.CommunicationPatternProperty;
import org.apache.nemo.common.ir.edge.executionproperty.DuplicateEdgeGroupProperty;
import org.apache.nemo.common.ir.edge.executionproperty.DuplicateEdgeGroupPropertyValue;
import org.apache.nemo.common.ir.executionproperty.EdgeExecutionProperty;
import org.apache.nemo.common.ir.executionproperty.ExecutionPropertyMap;
import org.apache.nemo.common.ir.vertex.IRVertex;
import org.apache.nemo.runtime.common.RuntimeIdManager;
import org.apache.nemo.runtime.common.plan.RuntimeEdge;
import org.apache.nemo.runtime.common.plan.StageEdge;
import org.apache.nemo.runtime.executor.data.BlockManagerWorker;
import org.apache.nemo.runtime.executor.data.DataUtil;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.function.Predicate;

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

    if (comValue.get().equals(CommunicationPatternProperty.Value.ONE_TO_ONE)) {
      return Collections.singletonList(readOneToOne());
    } else if (comValue.get().equals(CommunicationPatternProperty.Value.BROADCAST)) {
      return readBroadcast(index -> true);
    } else if (comValue.get().equals(CommunicationPatternProperty.Value.SHUFFLE)) {
      return readDataInRange(index -> true);
    } else {
      throw new UnsupportedCommPatternException(new Exception("Communication pattern not supported"));
    }
  }

  @Override
  public CompletableFuture<DataUtil.IteratorWithNumBytes> retry(final int desiredIndex) {
    final Optional<CommunicationPatternProperty.Value> comValue =
      runtimeEdge.getPropertyValue(CommunicationPatternProperty.class);

    if (comValue.get().equals(CommunicationPatternProperty.Value.ONE_TO_ONE)) {
      return readOneToOne();
    } else if (comValue.get().equals(CommunicationPatternProperty.Value.BROADCAST)) {
      return checkSingleElement(readBroadcast(index -> index == desiredIndex));
    } else if (comValue.get().equals(CommunicationPatternProperty.Value.SHUFFLE)) {
      return checkSingleElement(readDataInRange(index -> index == desiredIndex));
    } else {
      throw new UnsupportedCommPatternException(new Exception("Communication pattern not supported"));
    }
  }

  @Override
  public IRVertex getSrcIrVertex() {
    return srcVertex;
  }

  @Override
  public ExecutionPropertyMap<EdgeExecutionProperty> getProperties() {
    return runtimeEdge.getExecutionProperties();
  }

  private CompletableFuture<DataUtil.IteratorWithNumBytes> checkSingleElement(
    final List<CompletableFuture<DataUtil.IteratorWithNumBytes>> list) {
    if (list.size() != 1) {
      throw new IllegalArgumentException(list.toString());
    }
    return list.get(0);
  }

  /**
   * See {@link RuntimeIdManager#generateBlockIdWildcard(String, int)} for information on block wildcards.
   *
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
    return blockManagerWorker.readBlock(
      blockIdWildcard, runtimeEdge.getId(), runtimeEdge.getExecutionProperties(), HashRange.all());
  }

  private List<CompletableFuture<DataUtil.IteratorWithNumBytes>> readBroadcast(final Predicate<Integer> predicate) {
    final int numSrcTasks = InputReader.getSourceParallelism(this);
    final List<CompletableFuture<DataUtil.IteratorWithNumBytes>> futures = new ArrayList<>();
    for (int srcTaskIdx = 0; srcTaskIdx < numSrcTasks; srcTaskIdx++) {
      if (predicate.test(srcTaskIdx)) {
        final String blockIdWildcard = generateWildCardBlockId(srcTaskIdx);
        futures.add(blockManagerWorker.readBlock(
          blockIdWildcard, runtimeEdge.getId(), runtimeEdge.getExecutionProperties(), HashRange.all()));
      }
    }

    return futures;
  }

  /**
   * Read data in the assigned range of hash value.
   *
   * @return the list of the completable future of the data.
   */
  private List<CompletableFuture<DataUtil.IteratorWithNumBytes>> readDataInRange(final Predicate<Integer> predicate) {
    assert (runtimeEdge instanceof StageEdge);
    final List<KeyRange> keyRangeList = ((StageEdge) runtimeEdge).getKeyRanges();
    final KeyRange hashRangeToRead = keyRangeList.get(dstTaskIndex);
    if (hashRangeToRead == null) {
      throw new BlockFetchException(
        new Throwable("The hash range to read is not assigned to " + dstTaskIndex + "'th task"));
    }
    final int numSrcTasks = InputReader.getSourceParallelism(this);
    final List<CompletableFuture<DataUtil.IteratorWithNumBytes>> futures = new ArrayList<>();
    for (int srcTaskIdx = 0; srcTaskIdx < numSrcTasks; srcTaskIdx++) {
      if (predicate.test(srcTaskIdx)) {
        final String blockIdWildcard = generateWildCardBlockId(srcTaskIdx);
        futures.add(blockManagerWorker.readBlock(
          blockIdWildcard, runtimeEdge.getId(), runtimeEdge.getExecutionProperties(), hashRangeToRead));
      }
    }

    return futures;
  }
}
