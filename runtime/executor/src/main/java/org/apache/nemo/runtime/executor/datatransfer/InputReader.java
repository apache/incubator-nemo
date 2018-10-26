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
package org.apache.nemo.runtime.executor.datatransfer;

import com.google.common.annotations.VisibleForTesting;
import org.apache.nemo.common.ir.edge.executionproperty.CommunicationPatternProperty;
import org.apache.nemo.common.ir.edge.executionproperty.DuplicateEdgeGroupProperty;
import org.apache.nemo.common.ir.edge.executionproperty.DuplicateEdgeGroupPropertyValue;
import org.apache.nemo.common.ir.vertex.IRVertex;
import org.apache.nemo.common.ir.vertex.executionproperty.ParallelismProperty;
import org.apache.nemo.runtime.common.RuntimeIdManager;
import org.apache.nemo.runtime.common.plan.RuntimeEdge;
import org.apache.nemo.common.exception.UnsupportedCommPatternException;
import org.apache.nemo.runtime.executor.data.DataUtil;

import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

/**
 * Represents the input data transfer to a task.
 */
public abstract class InputReader extends DataTransfer {
  private final int dstTaskIndex;

  /**
   * Attributes that specify how we should read the input.
   */
  private final IRVertex srcVertex;
  private final RuntimeEdge runtimeEdge;

  InputReader(final int dstTaskIndex, final IRVertex srcVertex, final RuntimeEdge runtimeEdge) {
    super(runtimeEdge.getId());
    this.dstTaskIndex = dstTaskIndex;
    this.srcVertex = srcVertex;
    this.runtimeEdge = runtimeEdge;
  }

  /**
   * Reads input data depending on the communication pattern of the srcVertex.
   *
   * @return the read data.
   */
  public final List<CompletableFuture<DataUtil.IteratorWithNumBytes>> read() {
    final Optional<CommunicationPatternProperty.Value> comValue =
            runtimeEdge.getPropertyValue(CommunicationPatternProperty.class);

    if (comValue.get().equals(CommunicationPatternProperty.Value.OneToOne)) {
      return Collections.singletonList(readOneToOne());
    } else if (comValue.get().equals(CommunicationPatternProperty.Value.BroadCast)) {
      return readBroadcast();
    } else if (comValue.get().equals(CommunicationPatternProperty.Value.Shuffle)) {
      // If the dynamic optimization which detects data skew is enabled, read the data in the assigned range.
      // TODO #492: Modularize the data communication pattern.
      return readDataInRange();
    } else {
      throw new UnsupportedCommPatternException(new Exception("Communication pattern not supported"));
    }
  }

  abstract CompletableFuture<DataUtil.IteratorWithNumBytes> readOneToOne();
  abstract List<CompletableFuture<DataUtil.IteratorWithNumBytes>> readBroadcast();
  abstract List<CompletableFuture<DataUtil.IteratorWithNumBytes>> readDataInRange();

  /**
   * See {@link RuntimeIdManager#generateBlockIdWildcard(String, int)} for information on block wildcards.
   * @param producerTaskIndex to use.
   * @return wildcard block id that corresponds to "ANY" task attempt of the task index.
   */
  final String generateWildCardBlockId(final int producerTaskIndex) {
    final Optional<DuplicateEdgeGroupPropertyValue> duplicateDataProperty =
        runtimeEdge.getPropertyValue(DuplicateEdgeGroupProperty.class);
    if (!duplicateDataProperty.isPresent() || duplicateDataProperty.get().getGroupSize() <= 1) {
      return RuntimeIdManager.generateBlockIdWildcard(getId(), producerTaskIndex);
    }
    final String duplicateEdgeId = duplicateDataProperty.get().getRepresentativeEdgeId();
    return RuntimeIdManager.generateBlockIdWildcard(duplicateEdgeId, producerTaskIndex);
  }

  public final IRVertex getSrcIrVertex() {
    return srcVertex;
  }

  /**
   * Get the parallelism of the source task.
   *
   * @return the parallelism of the source task.
   */
  public final int getSourceParallelism() {
    return srcVertex.getPropertyValue(ParallelismProperty.class).
        orElseThrow(() -> new RuntimeException("No parallelism property on this edge."));
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
  public static Iterator combineFutures(final List<CompletableFuture<DataUtil.IteratorWithNumBytes>> futures)
      throws ExecutionException, InterruptedException {
    final List concatStreamBase = new ArrayList<>();
    Stream<Object> concatStream = concatStreamBase.stream();
    for (int srcTaskIdx = 0; srcTaskIdx < futures.size(); srcTaskIdx++) {
      final Iterator dataFromATask = futures.get(srcTaskIdx).get();
      final Iterable iterable = () -> dataFromATask;
      concatStream = Stream.concat(concatStream, StreamSupport.stream(iterable.spliterator(), false));
    }
    return concatStream.collect(Collectors.toList()).iterator();
  }

  final int getDstTaskIndex() {
    return this.dstTaskIndex;
  }

  final IRVertex getSrcVertex() {
    return this.srcVertex;
  }

  final RuntimeEdge getRuntimeEdge() {
    return this.runtimeEdge;
  }
}
