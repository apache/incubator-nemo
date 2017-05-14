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
import edu.snu.vortex.runtime.common.RuntimeAttribute;
import edu.snu.vortex.runtime.common.RuntimeAttributeMap;
import edu.snu.vortex.runtime.common.RuntimeIdGenerator;
import edu.snu.vortex.runtime.common.plan.RuntimeEdge;
import edu.snu.vortex.runtime.common.plan.logical.RuntimeVertex;
import edu.snu.vortex.runtime.common.plan.physical.Task;
import edu.snu.vortex.runtime.exception.UnsupportedCommPatternException;
import edu.snu.vortex.runtime.executor.block.BlockManagerWorker;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

/**
 * Represents the input data transfer to a task.
 */
public final class InputReader extends DataTransfer {
  private final int dstTaskIndex;

  private final BlockManagerWorker blockManagerWorker;

  /**
   * Attributes that specify how we should read the input.
   */
  private final RuntimeVertex srcRuntimeVertex;
  private final RuntimeEdge runtimeEdge;

  public InputReader(final int dstTaskIndex,
                     final RuntimeVertex srcRuntimeVertex,
                     final RuntimeEdge runtimeEdge,
                     final BlockManagerWorker blockManagerWorker) {

    super(runtimeEdge.getId());
    this.dstTaskIndex = dstTaskIndex;
    this.srcRuntimeVertex = srcRuntimeVertex;
    this.runtimeEdge = runtimeEdge;
    this.blockManagerWorker = blockManagerWorker;
  }

  /**
   * Reads input data depending on the communication pattern of the srcRuntimeVertex.
   *
   * @return the read data.
   */
  public Iterable<Element> read() {
    switch (runtimeEdge.getEdgeAttributes().get(RuntimeAttribute.Key.CommPattern)) {
      case OneToOne:
        return readOneToOne();
      case Broadcast:
        return readBroadcast();
      case ScatterGather:
        return readScatterGather();
      default:
        throw new UnsupportedCommPatternException(new Exception("Communication pattern not supported"));
    }
  }

  private Iterable<Element> readOneToOne() {
    final String blockId = RuntimeIdGenerator.generateBlockId(getId(), dstTaskIndex);
    return blockManagerWorker.getBlock(blockId,
        runtimeEdge.getEdgeAttributes().get(RuntimeAttribute.Key.BlockStore));
  }

  private Iterable<Element> readBroadcast() {
    final int numSrcTasks = srcRuntimeVertex.getVertexAttributes().get(RuntimeAttribute.IntegerKey.Parallelism);

    final List<Element> concatStreamBase = new ArrayList<>();
    Stream<Element> concatStream = concatStreamBase.stream();
    for (int srcTaskIdx = 0; srcTaskIdx < numSrcTasks; srcTaskIdx++) {
      final String blockId = RuntimeIdGenerator.generateBlockId(getId(), srcTaskIdx);
      final Iterable<Element> dataFromATask =
          blockManagerWorker.getBlock(blockId,
              runtimeEdge.getEdgeAttributes().get(RuntimeAttribute.Key.BlockStore));
      concatStream = Stream.concat(concatStream, StreamSupport.stream(dataFromATask.spliterator(), false));
    }
    return concatStream.collect(Collectors.toList());
  }

  private Iterable<Element> readScatterGather() {
    final int numSrcTasks = srcRuntimeVertex.getVertexAttributes().get(RuntimeAttribute.IntegerKey.Parallelism);

    final List<Element> concatStreamBase = new ArrayList<>();
    Stream<Element> concatStream = concatStreamBase.stream();
    for (int srcTaskIdx = 0; srcTaskIdx < numSrcTasks; srcTaskIdx++) {
      final String blockId = RuntimeIdGenerator.generateBlockId(getId(), srcTaskIdx, dstTaskIndex);
      final Iterable<Element> dataFromATask =
          blockManagerWorker.getBlock(blockId,
              runtimeEdge.getEdgeAttributes().get(RuntimeAttribute.Key.BlockStore));
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
    RuntimeAttributeMap edgeAttributes = runtimeEdge.getEdgeAttributes();

    return edgeAttributes.containsKey(RuntimeAttribute.Key.SideInput);
  }

  public Object getSideInput() {
    if (!isSideInputReader()) {
      throw new RuntimeException();
    }

    return read().iterator().next().getData();
  }
}
