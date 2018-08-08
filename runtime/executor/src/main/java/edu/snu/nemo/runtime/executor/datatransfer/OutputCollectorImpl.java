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
  private final ArrayList<O> mainTagOutputQueue; // Use ArrayList to allow 'null' values
  private final Map<String, ArrayList<Object>> additionalTagOutputQueues; // Use ArrayList to allow 'null' values

  /**
   * Constructor of a new OutputCollectorImpl with tagged outputs.
   * @param taggedChildren tagged children
   */
  public OutputCollectorImpl(final List<String> taggedChildren) {
    this.mainTagOutputQueue = new ArrayList<>(1);
    this.additionalTagOutputQueues = new HashMap<>();
    taggedChildren.forEach(child -> this.additionalTagOutputQueues.put(child, new ArrayList<>(1)));
  }

  @Override
  public void emit(final O output) {
    mainTagOutputQueue.add(output);
  }

  @Override
  public <T> void emit(final String dstVertexId, final T output) {
    if (this.additionalTagOutputQueues.get(dstVertexId) == null) {
      // This dstVertexId is for the main tag
      emit((O) output);
    } else {
      // Note that String#hashCode() can be cached, thus accessing additional output queues can be fast.
      this.additionalTagOutputQueues.get(dstVertexId).add(output);
    }
  }

  public Iterable<O> iterateMain() {
    return mainTagOutputQueue;
  }

  public Iterable<Object> iterateTag(final String tag) {
    if (this.additionalTagOutputQueues.get(tag) == null) {
      // This dstVertexId is for the main tag
      return (Iterable<Object>) iterateMain();
    } else {
      // Note that String#hashCode() can be cached, thus accessing additional output queues can be fast.
      return this.additionalTagOutputQueues.get(tag);
    }
  }

  public void clearMain() {
    mainTagOutputQueue.clear();
  }

  public void clearTag(final String tag) {
    if (this.additionalTagOutputQueues.get(tag) == null) {
      // This dstVertexId is for the main tag
      clearMain();
    } else {
      // Note that String#hashCode() can be cached, thus accessing additional output queues can be fast.
      this.additionalTagOutputQueues.get(tag).clear();
    }
  }
}
