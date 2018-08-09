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
  // Use ArrayList (not Queue) to allow 'null' values
  private final ArrayList<O> mainTagElements;
  private final Map<String, ArrayList<Object>> additionalTagElementsMap;

  /**
   * Constructor of a new OutputCollectorImpl with tagged outputs.
   * @param taggedChildren tagged children
   */
  public OutputCollectorImpl(final List<String> taggedChildren) {
    this.mainTagElements = new ArrayList<>(1);
    this.additionalTagElementsMap = new HashMap<>();
    taggedChildren.forEach(child -> this.additionalTagElementsMap.put(child, new ArrayList<>(1)));
  }

  @Override
  public void emit(final O output) {
    mainTagElements.add(output);
  }

  @Override
  public <T> void emit(final String dstVertexId, final T output) {
    if (this.additionalTagElementsMap.get(dstVertexId) == null) {
      // This dstVertexId is for the main tag
      emit((O) output);
    } else {
      // Note that String#hashCode() can be cached, thus accessing additional output queues can be fast.
      this.additionalTagElementsMap.get(dstVertexId).add(output);
    }
  }

  public Iterable<O> iterateMain() {
    return mainTagElements;
  }

  public Iterable<Object> iterateTag(final String tag) {
    if (this.additionalTagElementsMap.get(tag) == null) {
      // This dstVertexId is for the main tag
      return (Iterable<Object>) iterateMain();
    } else {
      // Note that String#hashCode() can be cached, thus accessing additional output queues can be fast.
      return this.additionalTagElementsMap.get(tag);
    }
  }

  public void clearMain() {
    mainTagElements.clear();
  }

  public void clearTag(final String tag) {
    if (this.additionalTagElementsMap.get(tag) == null) {
      // This dstVertexId is for the main tag
      clearMain();
    } else {
      // Note that String#hashCode() can be cached, thus accessing additional output queues can be fast.
      this.additionalTagElementsMap.get(tag).clear();
    }
  }
}
