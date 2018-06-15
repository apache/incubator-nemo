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
package edu.snu.nemo.runtime.executor.datatransfer;

import edu.snu.nemo.common.ir.OutputCollector;

import java.util.ArrayDeque;
import java.util.Queue;

/**
 * OutputCollector implementation.
 *
 * @param <O> output type.
 */
public final class OutputCollectorImpl<O> implements OutputCollector<O> {
  private final Queue<O> outputQueue;

  /**
   * Constructor of a new OutputCollectorImpl.
   */
  public OutputCollectorImpl() {
    this.outputQueue = new ArrayDeque<>(1);
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
}
