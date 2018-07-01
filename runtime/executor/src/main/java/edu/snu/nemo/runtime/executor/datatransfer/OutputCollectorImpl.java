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
import java.util.HashMap;
import java.util.Map;
import java.util.List;
import java.util.Queue;

/**
 * OutputCollector implementation.
 *
 * @param <O> output type.
 */
public final class OutputCollectorImpl<O> implements OutputCollector<O> {
  private final Queue<O> outputQueue;
  private final Map<String, Queue<Object>> taggedOutputQueues;

  /**
   * Constructor of a new OutputCollectorImpl.
   */
  public OutputCollectorImpl() {
    this.outputQueue = new ArrayDeque<>(1);
    this.taggedOutputQueues = new HashMap<>();
  }

  /**
   * Constructor of a new OutputCollectorImpl with tagged outputs.
   * @param taggedChildren tagged children
   */
  public OutputCollectorImpl(final List<String> taggedChildren) {
    this.outputQueue = new ArrayDeque<>(1);
    this.taggedOutputQueues = new HashMap<>();
    taggedChildren.forEach(child -> this.taggedOutputQueues.put(child, new ArrayDeque<>(1)));
  }

  @Override
  public void emit(final O output) {
    outputQueue.add(output);
  }

  @Override
  public <T> void emit(final String dstVertexId, final T output) {
    if (this.taggedOutputQueues.get(dstVertexId) == null) {
      emit((O) output);
    } else {
      this.taggedOutputQueues.get(dstVertexId).add(output);
    }
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
   * Inter-task data is transferred from sender-side Task's OutputCollectorImpl
   * to receiver-side Task.
   *
   * @param tag output tag
   * @return the first element of corresponding list
   */
  public Object remove(final String tag) {
    if (this.taggedOutputQueues.get(tag) == null) {
      return remove();
    } else {
      return this.taggedOutputQueues.get(tag).remove();
    }

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
   * Check if this OutputCollector is empty.
   *
   * @param tag output tag
   * @return true if this OutputCollector is empty.
   */
  public boolean isEmpty(final String tag) {
    if (this.taggedOutputQueues.get(tag) == null) {
      return isEmpty();
    } else {
      return this.taggedOutputQueues.get(tag).isEmpty();
    }
  }
}
