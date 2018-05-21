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

import edu.snu.nemo.common.ir.OutputCollector;
import edu.snu.nemo.runtime.common.plan.RuntimeEdge;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.List;

/**
 * OutputCollector implementation.
 *
 * @param <O> output type.
 */
public final class OutputCollectorImpl<O> implements OutputCollector<O> {
  private final ArrayDeque<O> outputQueue;
  private RuntimeEdge sideInputRuntimeEdge;
  private List<String> sideInputReceivers;

  /**
   * Constructor of a new OutputCollectorImpl.
   */
  public OutputCollectorImpl() {
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
    throw new UnsupportedOperationException("emit(dstVertexId, output) in OutputCollectorImpl.");
  }

  /**
   * Inter-Task data is transferred from sender-side Task's OutputCollectorImpl
   * to receiver-side Task.
   *
   * @return the first element of this list
   */
  public O remove() {
    return outputQueue.remove();
  }

  /**
   * Check if this OutputCollector is empty.
   *
   * @return true if this OutputCollector is empty.
   */
  public boolean isEmpty() {
    return outputQueue.isEmpty();
  }

  /**
   * Return the size of this OutputCollector.
   *
   * @return the total number of elements in this OutputCollector.
   */
  public int size() {
    return outputQueue.size();
  }

  /**
   * Mark this edge as side input so that TaskExecutor can retrieve
   * source transform using it.
   *
   * @param edge the RuntimeEdge to mark as side input.
   */
  public void setSideInputRuntimeEdge(final RuntimeEdge edge) {
    sideInputRuntimeEdge = edge;
  }

  /**
   * Get the RuntimeEdge marked as side input.
   *
   * @return the RuntimeEdge marked as side input.
   */
  public RuntimeEdge getSideInputRuntimeEdge() {
    return sideInputRuntimeEdge;
  }

  /**
   * Set this OutputCollector as having side input for the given child task.
   *
   * @param physicalTaskId the id of child task whose side input will be put into this OutputCollector.
   */
  public void setAsSideInputFor(final String physicalTaskId) {
    sideInputReceivers.add(physicalTaskId);
  }

  /**
   * Check if this OutputCollector has side input for the given child task.
   *
   * @return true if it contains side input for child task of the given id.
   */
  public boolean hasSideInputFor(final String physicalTaskId) {
    return sideInputReceivers.contains(physicalTaskId);
  }
}
