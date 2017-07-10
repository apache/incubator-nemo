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
import edu.snu.vortex.runtime.executor.data.partition.Partition;

import java.util.ArrayList;
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
  private final RuntimeVertex srcRuntimeVertex;
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
  public Iterable<Element> read() {
    try {
      switch (runtimeEdge.getEdgeAttributes().get(Attribute.Key.CommunicationPattern)) {
        case OneToOne:
          return readOneToOne();
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

  private Iterable<Element> readOneToOne() throws ExecutionException, InterruptedException {
    final String partitionId = RuntimeIdGenerator.generatePartitionId(getId(), dstTaskIndex);
    return partitionManagerWorker.getPartition(partitionId, getId(),
        runtimeEdge.getEdgeAttributes().get(Attribute.Key.ChannelDataPlacement)).get().asIterable();
  }

  private Iterable<Element> readBroadcast() throws ExecutionException, InterruptedException {
    final int numSrcTasks = srcRuntimeVertex.getVertexAttributes().get(Attribute.IntegerKey.Parallelism);

    final List<CompletableFuture<Partition>> futures = new ArrayList<>();
    for (int srcTaskIdx = 0; srcTaskIdx < numSrcTasks; srcTaskIdx++) {
      final String partitionId = RuntimeIdGenerator.generatePartitionId(getId(), srcTaskIdx);
      futures.add(partitionManagerWorker.getPartition(partitionId, getId(),
          runtimeEdge.getEdgeAttributes().get(Attribute.Key.ChannelDataPlacement)));
    }

    final List<Element> concatStreamBase = new ArrayList<>();
    Stream<Element> concatStream = concatStreamBase.stream();
    for (int srcTaskIdx = 0; srcTaskIdx < numSrcTasks; srcTaskIdx++) {
      final Iterable<Element> dataFromATask = futures.get(srcTaskIdx).get().asIterable();
      concatStream = Stream.concat(concatStream, StreamSupport.stream(dataFromATask.spliterator(), false));
    }
    return concatStream.collect(Collectors.toList());
  }

  private Iterable<Element> readScatterGather() throws ExecutionException, InterruptedException {
    final int numSrcTasks = srcRuntimeVertex.getVertexAttributes().get(Attribute.IntegerKey.Parallelism);

    final List<CompletableFuture<Partition>> futures = new ArrayList<>();
    for (int srcTaskIdx = 0; srcTaskIdx < numSrcTasks; srcTaskIdx++) {
      final String partitionId = RuntimeIdGenerator.generatePartitionId(getId(), srcTaskIdx, dstTaskIndex);
      futures.add(partitionManagerWorker.getPartition(partitionId, getId(),
          runtimeEdge.getEdgeAttributes().get(Attribute.Key.ChannelDataPlacement)));
    }

    final List<Element> concatStreamBase = new ArrayList<>();
    Stream<Element> concatStream = concatStreamBase.stream();
    for (int srcTaskIdx = 0; srcTaskIdx < numSrcTasks; srcTaskIdx++) {
      final Iterable<Element> dataFromATask = futures.get(srcTaskIdx).get().asIterable();
      concatStream = Stream.concat(concatStream, StreamSupport.stream(dataFromATask.spliterator(), false));
    }
    return concatStream.collect(Collectors.toList());
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

    return read().iterator().next().getData();
  }
}
