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
  private final String edgeId;
  private final int dstTaskIndex;

  private final BlockManagerWorker blockManagerWorker;

  /**
   * Attributes that specify how we should read the input.
   */
  private final RuntimeAttributeMap srcVertexAttributes;
  private final RuntimeAttributeMap edgeAttributes;

  public InputReader(final String edgeId,
                     final int dstTaskIndex,
                     final RuntimeAttributeMap srcVertexAttributes,
                     final RuntimeAttributeMap edgeAttributes,
                     final BlockManagerWorker blockManagerWorker) {
    super(edgeId);
    this.edgeId = edgeId;
    this.dstTaskIndex = dstTaskIndex;
    this.srcVertexAttributes = srcVertexAttributes;
    this.edgeAttributes = edgeAttributes;
    this.blockManagerWorker = blockManagerWorker;
  }

  /**
   * Reads input data depending on the communication pattern of the srcRuntimeVertex.
   *
   * @return the read data.
   */
  public Iterable<Element> read() {
    switch (edgeAttributes.get(RuntimeAttribute.Key.CommPattern)) {
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
    final String blockId = RuntimeIdGenerator.generateBlockId(edgeId, dstTaskIndex);
    return blockManagerWorker.getBlock(blockId, edgeAttributes.get(RuntimeAttribute.Key.BlockStore));
  }

  private Iterable<Element> readBroadcast() {
    final int numSrcTasks = srcVertexAttributes.get(RuntimeAttribute.IntegerKey.Parallelism);

    final List<Element> concatStreamBase = new ArrayList<>();
    Stream<Element> concatStream = concatStreamBase.stream();
    for (int srcTaskIdx = 0; srcTaskIdx < numSrcTasks; srcTaskIdx++) {
      final String blockId = RuntimeIdGenerator.generateBlockId(edgeId, srcTaskIdx);
      final Iterable<Element> dataFromATask =
          blockManagerWorker.getBlock(blockId, edgeAttributes.get(RuntimeAttribute.Key.BlockStore));
      concatStream = Stream.concat(concatStream, StreamSupport.stream(dataFromATask.spliterator(), false));
    }
    return concatStream.collect(Collectors.toList());
  }

  private Iterable<Element> readScatterGather() {
    final int numSrcTasks = srcVertexAttributes.get(RuntimeAttribute.IntegerKey.Parallelism);

    final List<Element> concatStreamBase = new ArrayList<>();
    Stream<Element> concatStream = concatStreamBase.stream();
    for (int srcTaskIdx = 0; srcTaskIdx < numSrcTasks; srcTaskIdx++) {
      final String blockId = RuntimeIdGenerator.generateBlockId(edgeId, srcTaskIdx, dstTaskIndex);
      final Iterable<Element> dataFromATask =
          blockManagerWorker.getBlock(blockId, edgeAttributes.get(RuntimeAttribute.Key.BlockStore));
      concatStream = Stream.concat(concatStream, StreamSupport.stream(dataFromATask.spliterator(), false));
    }
    return concatStream.collect(Collectors.toList());
  }
}
