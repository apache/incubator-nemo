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
  private final Set<String> mainTagOutputChildren;
  // Use ArrayList (not Queue) to allow 'null' values
  private final ArrayList<O> mainTagElements;
  private final Map<String, ArrayList<Object>> additionalTagElementsMap;

  /**
   * Constructor of a new OutputCollectorImpl with tagged outputs.
   * @param mainChildren   main children vertices
   * @param taggedChildren additional children vertices
   */
  public OutputCollectorImpl(final Set<String> mainChildren,
                             final List<String> taggedChildren) {
    this.mainTagOutputChildren = mainChildren;
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
    if (this.mainTagOutputChildren.contains(dstVertexId)) {
      // This dstVertexId is for the main tag
      emit((O) output);
    } else {
      // Note that String#hashCode() can be cached, thus accessing additional output queues can be fast.
      final List<Object> dataElements = this.additionalTagElementsMap.get(dstVertexId);
      if (dataElements == null) {
        throw new IllegalArgumentException("Wrong destination vertex id passed!");
      }
      dataElements.add(output);
    }
  }

  public Iterable<O> iterateMain() {
    return mainTagElements;
  }

  public Iterable<Object> iterateTag(final String tag) {
    if (this.mainTagOutputChildren.contains(tag)) {
      // This dstVertexId is for the main tag
      return (Iterable<Object>) iterateMain();
    } else {
      final List<Object> dataElements = this.additionalTagElementsMap.get(tag);
      if (dataElements == null) {
        throw new IllegalArgumentException("Wrong destination vertex id passed!");
      }
      return dataElements;
    }
  }

  public void clearMain() {
    mainTagElements.clear();
  }

  public void clearTag(final String tag) {
    if (this.mainTagOutputChildren.contains(tag)) {
      // This dstVertexId is for the main tag
      clearMain();
    } else {
      // Note that String#hashCode() can be cached, thus accessing additional output queues can be fast.
      final List<Object> dataElements = this.additionalTagElementsMap.get(tag);
      if (dataElements == null) {
        throw new IllegalArgumentException("Wrong destination vertex id passed!");
      }
      dataElements.clear();
    }
  }

  public List<O> getMainTagOutputQueue() {
    return mainTagElements;
  }

  public List<Object> getAdditionalTagOutputQueue(final String dstVertexId) {
    if (this.mainTagOutputChildren.contains(dstVertexId)) {
      return (List<Object>) this.mainTagElements;
    } else {
      final List<Object> dataElements = this.additionalTagElementsMap.get(dstVertexId);
      if (dataElements == null) {
        throw new IllegalArgumentException("Wrong destination vertex id passed!");
      }
      return dataElements;
    }
  }
}
