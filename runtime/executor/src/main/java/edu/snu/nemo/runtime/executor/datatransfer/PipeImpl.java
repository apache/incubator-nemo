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
package edu.snu.nemo.runtime.executor.datatransfer;

import edu.snu.nemo.common.ir.Pipe;
import edu.snu.nemo.runtime.common.plan.RuntimeEdge;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.List;
import java.util.Queue;

/**
 * Pipe implementation that requires synchronization.
 * @param <O> output type.
 */
public final class PipeImpl<O> implements Pipe<O> {
  private static final Logger LOG = LoggerFactory.getLogger(PipeImpl.class.getName());
  private final Queue<O> outputQueue;
  private RuntimeEdge sideInputRuntimeEdge;
  private List<String> sideInputReceivers;

  /**
   * Constructor of a new Pipe.
   */
  public PipeImpl() {
    this.outputQueue = new ArrayDeque<>();
    this.sideInputRuntimeEdge = null;
    this.sideInputReceivers = new ArrayList<>();
  }

  @Override
  public void emit(final O output) {
    outputQueue.add(output);
  }

  @Override
  public void emit(final String dstVertexId, final Object output) {
    throw new UnsupportedOperationException("emit(dstVertexId, output) in PipeImpl.");
  }

  /**
   * Inter-Task data is transferred from sender-side Task's PipeImpl to receiver-side Task.
   * @return the first element of this list
   */
  public O remove() {
    final O element = outputQueue.remove();
    LOG.info("Read element: {}", element);
    return element;
  }

  public boolean isEmpty() {
    return outputQueue.isEmpty();
  }

  public int size() {
    return outputQueue.size();
  }

  public void setSideInputRuntimeEdge(final RuntimeEdge edge) {
    sideInputRuntimeEdge = edge;
  }

  public RuntimeEdge getSideInputRuntimeEdge() {
    return sideInputRuntimeEdge;
  }

  public void setAsSideInput(final String physicalTaskId) {
    sideInputReceivers.add(physicalTaskId);
  }

  public boolean hasSideInputFor(final String physicalTaskId) {
    return sideInputReceivers.contains(physicalTaskId);
  }
}
