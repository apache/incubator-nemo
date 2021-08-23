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

import org.apache.beam.repackaged.core.org.apache.commons.lang3.SerializationUtils;
import org.apache.nemo.common.HashRange;
import org.apache.nemo.common.KeyRange;
import org.apache.nemo.common.exception.BlockFetchException;
import org.apache.nemo.common.exception.UnsupportedCommPatternException;
import org.apache.nemo.common.ir.edge.executionproperty.CommunicationPatternProperty;
import org.apache.nemo.common.ir.edge.executionproperty.DuplicateEdgeGroupProperty;
import org.apache.nemo.common.ir.edge.executionproperty.DuplicateEdgeGroupPropertyValue;
import org.apache.nemo.common.ir.edge.executionproperty.PartitionerProperty;
import org.apache.nemo.common.ir.executionproperty.EdgeExecutionProperty;
import org.apache.nemo.common.ir.executionproperty.ExecutionPropertyMap;
import org.apache.nemo.common.ir.vertex.IRVertex;
import org.apache.nemo.common.ir.vertex.executionproperty.EnableWorkStealingProperty;
import org.apache.nemo.common.ir.vertex.executionproperty.WorkStealingSubSplitProperty;
import org.apache.nemo.runtime.common.RuntimeIdManager;
import org.apache.nemo.runtime.common.plan.RuntimeEdge;
import org.apache.nemo.runtime.common.plan.StageEdge;
import org.apache.nemo.runtime.executor.MetricMessageSender;
import org.apache.nemo.runtime.executor.data.BlockManagerWorker;
import org.apache.nemo.runtime.executor.data.DataUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
  private static final Logger LOG = LoggerFactory.getLogger(BlockInputReader.class.getName());
  private final BlockManagerWorker blockManagerWorker;
  private final MetricMessageSender metricMessageSender;
  private final String dstTaskId;
  private final int dstTaskIndex;

  private static final String SPLIT_STRATEGY = "SPLIT";
  private static final String MERGE_STRATEGY = "MERGE";
  private static final String DEFAULT_STRATEGY = "DEFAULT";

  /**
   * Attributes that specify how we should read the input.
   */
  private final IRVertex srcVertex;
  private final RuntimeEdge runtimeEdge;

  BlockInputReader(final String dstTaskId,
                   final IRVertex srcVertex,
                   final RuntimeEdge runtimeEdge,
                   final BlockManagerWorker blockManagerWorker,
                   final MetricMessageSender metricMessageSender) {
    this.dstTaskId = dstTaskId;
    this.dstTaskIndex = RuntimeIdManager.getIndexFromTaskId(dstTaskId);
    this.srcVertex = srcVertex;
    this.runtimeEdge = runtimeEdge;
    this.blockManagerWorker = blockManagerWorker;
    this.metricMessageSender = metricMessageSender;
  }

  @Override
  public List<CompletableFuture<DataUtil.IteratorWithNumBytes>> read() {
    final Optional<CommunicationPatternProperty.Value> comValueOptional =
      runtimeEdge.getPropertyValue(CommunicationPatternProperty.class);
    final CommunicationPatternProperty.Value comValue = comValueOptional.orElseThrow(IllegalStateException::new);

    switch (comValue) {
      case ONE_TO_ONE:
        return Collections.singletonList(readOneToOne());
      case BROADCAST:
        return readBroadcast(index -> true, Optional.empty(), 1);
      case SHUFFLE:
        return readDataInRange(index -> true, Optional.empty(), 1);
      default:
        throw new UnsupportedCommPatternException(new Exception("Communication pattern not supported"));
    }
  }

  @Override
  public List<CompletableFuture<DataUtil.IteratorWithNumBytes>> read(final String workStealingState,
                                                                     final int numSubSplit,
                                                                     final int subSplitIndex) {
    if (workStealingState.equals(MERGE_STRATEGY)
      && srcVertex.getPropertyValue(EnableWorkStealingProperty.class).orElse(DEFAULT_STRATEGY)
        .equals(SPLIT_STRATEGY)) {
      return readSplitBlocks(InputReader.getSourceParallelism(this),
        srcVertex.getPropertyValue(WorkStealingSubSplitProperty.class).orElse(1));
    } else {
      List<CompletableFuture<DataUtil.IteratorWithNumBytes>> futures = read();

      if (!workStealingState.equals(SPLIT_STRATEGY)) {
        /* DEFAULT case */
        LOG.error("DEFAULT CASE: size {}", futures.size());
        return futures;
      }

      /* SPLIT case*/
      // 가능한 even 하게 나누는 방법 생각해서 보완
      // 19를 10개로 나누는 건 1*9, 10*1 보다는 2*9, 1*1 이 나음'
      final int leftInterval = subSplitIndex * (futures.size() / numSubSplit);
      final int rightInterval = numSubSplit == subSplitIndex + 1
        ? futures.size() : (subSplitIndex + 1) * (futures.size() / numSubSplit);
      LOG.error("SPLIT CASE: future.size: {}", futures.size());
      LOG.error("SPLIT CASE: numSubSplit, index: {}, {}", numSubSplit, subSplitIndex);
      LOG.error("SPLIT CASE: [{}, {})", leftInterval, rightInterval);
      return futures.subList(leftInterval, rightInterval);
    }
  }


  @Override
  public CompletableFuture<DataUtil.IteratorWithNumBytes> retry(final int desiredIndex) {
    final Optional<CommunicationPatternProperty.Value> comValueOptional =
      runtimeEdge.getPropertyValue(CommunicationPatternProperty.class);
    final CommunicationPatternProperty.Value comValue = comValueOptional.orElseThrow(IllegalStateException::new);

    switch (comValue) {
      case ONE_TO_ONE:
        return readOneToOne();
      case BROADCAST:
        return checkSingleElement(readBroadcast(index -> index == desiredIndex, Optional.empty(), 1));
      case SHUFFLE:
        return checkSingleElement(readDataInRange(index -> index == desiredIndex, Optional.empty(), 1));
      default:
        throw new UnsupportedCommPatternException(new Exception("Communication pattern not supported"));
    }
  }

  @Override
  public CompletableFuture<DataUtil.IteratorWithNumBytes> retry(final String workStealingState,
                                                                final int numSubSplit,
                                                                final int desiredIndex) {

    final boolean isMergeAfterSplit = workStealingState.equals(MERGE_STRATEGY)
      && srcVertex.getPropertyValue(EnableWorkStealingProperty.class).orElse(DEFAULT_STRATEGY)
        .equals(SPLIT_STRATEGY);

    if (!isMergeAfterSplit && !workStealingState.equals(SPLIT_STRATEGY)) {
      return retry(desiredIndex);
    }

    final int srcParallelism = InputReader.getSourceParallelism(this);
    final int srcNumSubSplit = srcVertex.getPropertyValue(WorkStealingSubSplitProperty.class).orElse(1);
    final int trueIndex = translateIndex(workStealingState, numSubSplit, desiredIndex);
    final int subIndex =  isMergeAfterSplit ? desiredIndex % srcNumSubSplit : 0;

    final Optional<CommunicationPatternProperty.Value> comValueOptional =
      runtimeEdge.getPropertyValue(CommunicationPatternProperty.class);
    final CommunicationPatternProperty.Value comValue = comValueOptional.orElseThrow(IllegalStateException::new);

    switch (comValue) {
      case ONE_TO_ONE:
        return readOneToOne();
      case BROADCAST:
        return readBroadcast(index -> index == trueIndex,
          isMergeAfterSplit ? Optional.of(srcParallelism) : Optional.empty(),
          isMergeAfterSplit ? srcNumSubSplit : 1).get(subIndex);
      case SHUFFLE:
        return readDataInRange(index -> index == trueIndex,
          isMergeAfterSplit ? Optional.of(srcParallelism) : Optional.empty(),
          isMergeAfterSplit ? srcNumSubSplit : 1).get(subIndex);
      default:
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
   * See {@link RuntimeIdManager#generateBlockIdWildcard(String, int, String)} for information on block wildcards.
   *
   * @param producerTaskIndex to use.
   * @return wildcard block id that corresponds to "ANY" task attempt of the task index.
   */
  private String generateWildCardBlockId(final int producerTaskIndex,
                                         final String subSplitIndex) {
    final Optional<DuplicateEdgeGroupPropertyValue> duplicateDataProperty =
      runtimeEdge.getPropertyValue(DuplicateEdgeGroupProperty.class);
    if (!duplicateDataProperty.isPresent() || duplicateDataProperty.get().getGroupSize() <= 1) {
      return RuntimeIdManager.generateBlockIdWildcard(runtimeEdge.getId(), producerTaskIndex, subSplitIndex);
    }
    final String duplicateEdgeId = duplicateDataProperty.get().getRepresentativeEdgeId();
    return RuntimeIdManager.generateBlockIdWildcard(duplicateEdgeId, producerTaskIndex, subSplitIndex);
  }

  private CompletableFuture<DataUtil.IteratorWithNumBytes> readOneToOne() {
    final String blockIdWildcard = generateWildCardBlockId(dstTaskIndex, "*");
    return blockManagerWorker.readBlock(
      blockIdWildcard, runtimeEdge.getId(), runtimeEdge.getExecutionProperties(), HashRange.all());
  }

  /**
   * Read data in full range of hash value.
   * if @param{numSubSplit} > 1, it indicates that it is reading sub-split blocks from work stealing.
   *
   * @return the list of the completable future of the data.
   */
  private List<CompletableFuture<DataUtil.IteratorWithNumBytes>> readBroadcast(final Predicate<Integer> predicate,
                                                                               final Optional<Integer> numSrcIndex,
                                                                               final int numSubSplit) {
    final int numSrcTasks = numSrcIndex.orElse(InputReader.getSourceParallelism(this));
    final List<CompletableFuture<DataUtil.IteratorWithNumBytes>> futures = new ArrayList<>();
    for (int srcTaskIdx = 0; srcTaskIdx < numSrcTasks; srcTaskIdx++) {
      if (predicate.test(srcTaskIdx)) {
        for (int subSplitIdx = 0; subSplitIdx < numSubSplit; subSplitIdx++) {
          final String blockIdWildcard = generateWildCardBlockId(srcTaskIdx,
            numSubSplit == 1 ? "*" : Integer.toString(subSplitIdx));
          futures.add(blockManagerWorker.readBlock(
            blockIdWildcard, runtimeEdge.getId(), runtimeEdge.getExecutionProperties(), HashRange.all()));
        }
      }
    }
    return futures;
  }

  /**
   * Read data in the assigned range of hash value.
   * if @param{numSubSplit} > 1, it indicates that it is reading sub-split blocks from work stealing.
   *
   * @return the list of the completable future of the data.
   */
  private List<CompletableFuture<DataUtil.IteratorWithNumBytes>> readDataInRange(final Predicate<Integer> predicate,
                                                                                 final Optional<Integer> numSrcIndex,
                                                                                 final int numSubSplit) {
    assert (runtimeEdge instanceof StageEdge);
    final List<KeyRange> keyRangeList = ((StageEdge) runtimeEdge).getKeyRanges();
    final KeyRange hashRangeToRead = keyRangeList.get(dstTaskIndex);
    if (hashRangeToRead == null) {
      throw new BlockFetchException(
        new Throwable("The hash range to read is not assigned to " + dstTaskIndex + "'th task"));
    }
    final int partitionerProperty = ((StageEdge) runtimeEdge).getPropertyValue(PartitionerProperty.class).get().right();
    final int taskSize = ((HashRange) hashRangeToRead).rangeEndExclusive()
      - ((HashRange) hashRangeToRead).rangeBeginInclusive();
    metricMessageSender.send("TaskMetric", dstTaskId, "taskSizeRatio",
      SerializationUtils.serialize(partitionerProperty / taskSize));
    final int numSrcTasks = numSrcIndex.orElse(InputReader.getSourceParallelism(this));
    final List<CompletableFuture<DataUtil.IteratorWithNumBytes>> futures = new ArrayList<>();
    for (int srcTaskIdx = 0; srcTaskIdx < numSrcTasks; srcTaskIdx++) {
      if (predicate.test(srcTaskIdx)) {
        for (int subSplitIdx = 0; subSplitIdx < numSubSplit; subSplitIdx++) {
          final String blockIdWildcard = generateWildCardBlockId(srcTaskIdx,
            numSubSplit == 1 ? "*" : Integer.toString(subSplitIdx));
          futures.add(blockManagerWorker.readBlock(
            blockIdWildcard, runtimeEdge.getId(), runtimeEdge.getExecutionProperties(), hashRangeToRead));
        }
      }
    }
    return futures;
  }

  /**
   * Read sub-split blocks generated by work stealing SPLIT stage.
   * @param srcParallelism  src stage parallelism.
   * @param srcNumSubSplit  number of sub-split blocks per src task index.
   * @return                List of iterators.
   */
  private List<CompletableFuture<DataUtil.IteratorWithNumBytes>> readSplitBlocks(final int srcParallelism,
                                                                                 final int srcNumSubSplit) {
    final Optional<CommunicationPatternProperty.Value> comValueOptional =
      runtimeEdge.getPropertyValue(CommunicationPatternProperty.class);
    final CommunicationPatternProperty.Value comValue = comValueOptional.orElseThrow(IllegalStateException::new);

    switch (comValue) {
      case ONE_TO_ONE:
        return Collections.singletonList(readOneToOne());
      case BROADCAST:
        return readBroadcast(index -> true, Optional.of(srcParallelism), srcNumSubSplit);
      case SHUFFLE:
        return readDataInRange(index -> true, Optional.of(srcParallelism), srcNumSubSplit);
      default:
        throw new UnsupportedCommPatternException(new Exception("Communication pattern not supported"));
    }
  }

  private int translateIndex(final String workStealingState,
                             final int numSubSplit,
                             final int index) {
    // 여기서는 source vertex 의 parallelism 이 훨씬 중요함.
    if (workStealingState.equals(SPLIT_STRATEGY)) {
      // source parallelism, current sub split num, current sub split index, parameter index
      int sourceParallelism = InputReader.getSourceParallelism(this);
      return Math.round(sourceParallelism / numSubSplit) * RuntimeIdManager.getPartialFromTaskId(dstTaskId) + index;
    } else if (workStealingState.equals(MERGE_STRATEGY)
      && srcVertex.getPropertyValue(EnableWorkStealingProperty.class).orElse(DEFAULT_STRATEGY)
          .equals(SPLIT_STRATEGY)) {
      // src parallelism, src original parallelism, source sub split num
      int srcNumSubSplit = srcVertex.getPropertyValue(WorkStealingSubSplitProperty.class).orElse(1);
      return index / srcNumSubSplit;
    } else {
      return index;
    }
  }
}
