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
package edu.snu.vortex.runtime.executor.datatransfer;

import edu.snu.vortex.compiler.ir.Element;
import edu.snu.vortex.compiler.ir.attribute.Attribute;
import edu.snu.vortex.compiler.ir.attribute.AttributeMap;
import edu.snu.vortex.runtime.common.RuntimeIdGenerator;
import edu.snu.vortex.runtime.common.plan.RuntimeEdge;
import edu.snu.vortex.runtime.common.plan.logical.RuntimeVertex;
import edu.snu.vortex.runtime.common.plan.physical.Task;
import edu.snu.vortex.runtime.exception.PartitionFetchException;
import edu.snu.vortex.runtime.exception.UnsupportedCommPatternException;
import edu.snu.vortex.runtime.executor.data.PartitionManagerWorker;

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
 */
public final class InputReader extends DataTransfer {
  private final int dstTaskIndex;

  private final PartitionManagerWorker partitionManagerWorker;

  /**
   * Attributes that specify how we should read the input.
   */
  @Nullable
  private final RuntimeVertex srcRuntimeVertex; // This vertex is null if this reader is a local reader.
  private final RuntimeEdge runtimeEdge;

  public InputReader(final int dstTaskIndex,
                     final RuntimeVertex srcRuntimeVertex,
                     final RuntimeEdge runtimeEdge,
                     final PartitionManagerWorker partitionManagerWorker) {

    super(runtimeEdge.getId());
    this.dstTaskIndex = dstTaskIndex;
    this.srcRuntimeVertex = srcRuntimeVertex;
    this.runtimeEdge = runtimeEdge;
    this.partitionManagerWorker = partitionManagerWorker;
  }

  /**
   * Reads input data depending on the communication pattern of the srcRuntimeVertex.
   *
   * @return the read data.
   */
  public List<CompletableFuture<Iterable<Element>>> read() {
    try {
      switch (runtimeEdge.getEdgeAttributes().get(Attribute.Key.CommunicationPattern)) {
        case OneToOne:
          return Collections.singletonList(readOneToOne());
        case Broadcast:
          return readBroadcast();
        case ScatterGather:
          return readScatterGather();
        default:
          throw new UnsupportedCommPatternException(new Exception("Communication pattern not supported"));
      }
    } catch (InterruptedException | ExecutionException e) {
      throw new PartitionFetchException(e);
    }
  }

  private CompletableFuture<Iterable<Element>> readOneToOne() throws ExecutionException, InterruptedException {
    final String partitionId = RuntimeIdGenerator.generatePartitionId(getId(), dstTaskIndex);
    return partitionManagerWorker.getPartition(partitionId, getId(),
        runtimeEdge.getEdgeAttributes().get(Attribute.Key.ChannelDataPlacement));
  }

  private List<CompletableFuture<Iterable<Element>>> readBroadcast()
      throws ExecutionException, InterruptedException {
    final int numSrcTasks = this.getSourceParallelism();

    final List<CompletableFuture<Iterable<Element>>> futures = new ArrayList<>();
    for (int srcTaskIdx = 0; srcTaskIdx < numSrcTasks; srcTaskIdx++) {
      final String partitionId = RuntimeIdGenerator.generatePartitionId(getId(), srcTaskIdx);
      futures.add(partitionManagerWorker.getPartition(partitionId, getId(),
          runtimeEdge.getEdgeAttributes().get(Attribute.Key.ChannelDataPlacement)));
    }

    return futures;
  }

  private List<CompletableFuture<Iterable<Element>>> readScatterGather()
      throws ExecutionException, InterruptedException {
    final int numSrcTasks = this.getSourceParallelism();

    final List<CompletableFuture<Iterable<Element>>> futures = new ArrayList<>();
    for (int srcTaskIdx = 0; srcTaskIdx < numSrcTasks; srcTaskIdx++) {
      final String partitionId = RuntimeIdGenerator.generatePartitionId(getId(), srcTaskIdx, dstTaskIndex);
      futures.add(partitionManagerWorker.getPartition(partitionId, getId(),
          runtimeEdge.getEdgeAttributes().get(Attribute.Key.ChannelDataPlacement)));
    }

    return futures;
  }

  public RuntimeEdge getRuntimeEdge() {
    return runtimeEdge;
  }

  public String getSrcRuntimeVertexId() {
    // this src runtime vertex can be either a real vertex or a task. we must check!
    if (srcRuntimeVertex != null) {
      return srcRuntimeVertex.getId();
    }

    return ((Task) runtimeEdge.getSrc()).getRuntimeVertexId();
  }

  public boolean isSideInputReader() {
    AttributeMap edgeAttributes = runtimeEdge.getEdgeAttributes();

    return edgeAttributes.containsKey(Attribute.Key.SideInput);
  }

  public Object getSideInput() {
    if (!isSideInputReader()) {
      throw new RuntimeException();
    }
    final List<CompletableFuture<Iterable<Element>>> futures = this.read();

    try {
      return futures.get(0).get().iterator().next().getData();
    } catch (InterruptedException | ExecutionException e) {
      throw new PartitionFetchException(e);
    }
  }

  /**
   * Get the parallelism of the source task.
   *
   * @return the parallelism of the source task.
   */
  public int getSourceParallelism() {
    if (srcRuntimeVertex != null) {
      final Integer numSrcTasks = srcRuntimeVertex.getVertexAttributes().get(Attribute.IntegerKey.Parallelism);
      return numSrcTasks == null ? 1 : numSrcTasks;
    } else {
      // Local input reader
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
  public static Iterable<Element> combineFutures(final List<CompletableFuture<Iterable<Element>>> futures)
      throws ExecutionException, InterruptedException {
    final List<Element> concatStreamBase = new ArrayList<>();
    Stream<Element> concatStream = concatStreamBase.stream();
    for (int srcTaskIdx = 0; srcTaskIdx < futures.size(); srcTaskIdx++) {
      final Iterable<Element> dataFromATask = futures.get(srcTaskIdx).get();
      concatStream = Stream.concat(concatStream, StreamSupport.stream(dataFromATask.spliterator(), false));
    }
    return concatStream.collect(Collectors.toList());
  }
}
