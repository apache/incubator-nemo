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

import java.util.*;

/**
 * OutputCollector implementation.
 *
 * @param <O> output type.
 */
public final class OutputCollectorImpl<O> implements OutputCollector<O> {
  private final Optional<String> mainTag;
  private final Set<String> mainTagOutputChildren;
  private final Queue<O> mainTagOutputQueue;
  private final Map<String, Queue<Object>> taggedOutputQueues;

  /**
   * Constructor of a new OutputCollectorImpl with tagged outputs.
   * @param mainChildren       main children vertices
   * @param additionalChildren additional children vertices
   */
  public OutputCollectorImpl(final Optional<String> mainTag,
                             final Set<String> mainChildren,
                             final List<String> additionalChildren) {
    this.mainTag = mainTag;
    this.mainTagOutputChildren = mainChildren;
    this.mainTagOutputQueue = new ArrayDeque<>(1);
    this.taggedOutputQueues = new HashMap<>();

    additionalChildren.forEach(child -> this.taggedOutputQueues.put(child, new ArrayDeque<>(1)));
  }

  @Override
  public void emit(final O output) {
    mainTagOutputQueue.add(output);
  }

  @Override
  public <T> void emit(final String dstVertexId, final T output) {
    if (this.mainTagOutputChildren.contains(dstVertexId)) {
      // This dstVertexId is for the main tag
      emit((O) output);
    } else {
      // Note that String#hashCode() can be cached, thus accessing additional output queues can be fast.
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
    return mainTagOutputQueue.remove();
  }

  /**
   * Inter-task data is transferred from sender-side Task's OutputCollectorImpl
   * to receiver-side Task.
   *
   * @param tag output tag
   * @return the first element of corresponding list
   */
  public Object remove(final String tag) {
    if (this.mainTagOutputChildren.contains(tag)) {
      // This dstVertexId is for the main tag
      return remove();
    } else {
      // Note that String#hashCode() can be cached, thus accessing additional output queues can be fast.
      return this.taggedOutputQueues.get(tag).remove();
    }

  }

  /**
   * Check if this OutputCollector is empty.
   *
   * @return true if this OutputCollector is empty.
   */
  public boolean isEmpty() {
    return mainTagOutputQueue.isEmpty();
  }

  /**
   * Check if this OutputCollector is empty.
   *
   * @param tag output tag
   * @return true if this OutputCollector is empty.
   */
  public boolean isEmpty(final String tag) {
    if (this.mainTagOutputChildren.contains(tag)) {
      return isEmpty();
    } else {
      // Note that String#hashCode() can be cached, thus accessing additional output queues can be fast.
      return this.taggedOutputQueues.get(tag).isEmpty();
    }
  }

  public Optional<String> getMainTag() {
    return mainTag;
  }

  public Queue<O> getMainTagOutputQueue() {
    return mainTagOutputQueue;
  }

  public Queue getAdditionalTagOutputQueue(final String dstVertexId) {
    if (this.mainTagOutputChildren.contains(dstVertexId)) {
      return this.mainTagOutputQueue;
    } else {
      return this.taggedOutputQueues.get(dstVertexId);
    }
  }
}
