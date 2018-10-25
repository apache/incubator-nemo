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
package org.apache.nemo.runtime.executor.datatransfer;

import org.apache.nemo.common.Pair;
import org.apache.nemo.common.ir.OutputCollector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

/**
 * OutputCollector implementation.
 *
 * @param <O> output type.
 */
public final class OutputCollectorImpl<O> implements OutputCollector<O> {
  private static final Logger LOG = LoggerFactory.getLogger(OutputCollectorImpl.class.getName());
  private final Set<String> mainTagOutputChildren;
  // Use ArrayList (not Queue) to allow 'null' values
  private final ArrayList<O> mainTagElements;
  // Key: Pair of tag and destination vertex id
  // Value: data elements which will be input to the tagged destination vertex
  private final Map<Pair<String, String>, ArrayList<Object>> additionalTaggedChildToElementsMap;

  /**
   * Constructor of a new OutputCollectorImpl with tagged outputs.
   * @param mainChildren   main children vertices
   * @param tagToChildren additional children vertices
   */
  public OutputCollectorImpl(final Set<String> mainChildren,
                             final Map<String, String> tagToChildren) {
    this.mainTagOutputChildren = mainChildren;
    this.mainTagElements = new ArrayList<>(1);
    this.additionalTaggedChildToElementsMap = new HashMap<>();
    tagToChildren.forEach((tag, child) ->
      this.additionalTaggedChildToElementsMap.put(Pair.of(tag, child), new ArrayList<>(1)));
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
      final List<Object> dataElements = getAdditionalTaggedDataFromDstVertexId(dstVertexId);
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
      return getAdditionalTaggedDataFromTag(tag);
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
      final List<Object> dataElements = getAdditionalTaggedDataFromTag(tag);
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
      return getAdditionalTaggedDataFromDstVertexId(dstVertexId);
    }
  }

  private List<Object> getAdditionalTaggedDataFromDstVertexId(final String dstVertexId) {
    final Pair<String, String> tagAndChild =
      this.additionalTaggedChildToElementsMap.keySet().stream()
        .filter(key -> key.right().equals(dstVertexId))
        .findAny().orElseThrow(() -> new RuntimeException("Wrong destination vertex id passed!"));
    final List<Object> dataElements = this.additionalTaggedChildToElementsMap.get(tagAndChild);
    if (dataElements == null) {
      throw new IllegalArgumentException("Wrong destination vertex id passed!");
    }
    return dataElements;
  }

  private List<Object> getAdditionalTaggedDataFromTag(final String tag) {
    final Pair<String, String> tagAndChild =
      this.additionalTaggedChildToElementsMap.keySet().stream()
        .filter(key -> key.left().equals(tag))
        .findAny().orElseThrow(() -> new RuntimeException("Wrong tag " + tag + " passed!"));
    final List<Object> dataElements = this.additionalTaggedChildToElementsMap.get(tagAndChild);
    if (dataElements == null) {
      throw new IllegalArgumentException("Wrong tag " + tag + " passed!");
    }
    return dataElements;
  }
}
