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
package edu.snu.nemo.common.ir.executionproperty;

import edu.snu.nemo.common.ir.edge.IREdge;
import edu.snu.nemo.common.ir.edge.executionproperty.DataFlowModelProperty;
import edu.snu.nemo.common.ir.edge.executionproperty.InterTaskDataStoreProperty;
import edu.snu.nemo.common.ir.edge.executionproperty.PartitionerProperty;
import edu.snu.nemo.common.ir.vertex.IRVertex;
import edu.snu.nemo.common.ir.edge.executionproperty.DataCommunicationPatternProperty;
import edu.snu.nemo.common.ir.vertex.executionproperty.ExecutorPlacementProperty;
import edu.snu.nemo.common.ir.vertex.executionproperty.ParallelismProperty;

import com.google.common.annotations.VisibleForTesting;
import org.apache.commons.lang3.builder.HashCodeBuilder;

import javax.annotation.concurrent.NotThreadSafe;
import java.io.Serializable;
import java.util.*;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * ExecutionPropertyMap Class, which uses HashMap for keeping track of ExecutionProperties for vertices and edges.
 * @param <T> Type of {@link ExecutionProperty} this map stores.
 */
@NotThreadSafe
public final class ExecutionPropertyMap<T extends ExecutionProperty> implements Serializable {
  private final String id;
  private Map<Class<? extends ExecutionProperty>, T> properties = new HashMap<>();

  /**
   * Constructor for ExecutionPropertyMap class.
   * @param id ID of the vertex / edge to keep the execution property of.
   */
  @VisibleForTesting
  public ExecutionPropertyMap(final String id) {
    this.id = id;
  }

  public ExecutionPropertyMap(final String id, final Map<Class<? extends ExecutionProperty>, T> properties) {
    this.id = id;
    this.properties = properties;
  }

  public ExecutionPropertyMap getCopy() {
    return new ExecutionPropertyMap(this.id, this.properties);
  }

  /**
   * Static initializer for irEdges.
   * @param irEdge irEdge to keep the execution property of.
   * @param commPattern Data communication pattern type of the edge.
   * @return The corresponding ExecutionPropertyMap.
   */
  public static ExecutionPropertyMap<EdgeExecutionProperty> of(
      final IREdge irEdge,
      final DataCommunicationPatternProperty.Value commPattern) {
    final ExecutionPropertyMap<EdgeExecutionProperty> map = new ExecutionPropertyMap<>(irEdge.getId());
    map.put(DataCommunicationPatternProperty.of(commPattern));
    map.put(DataFlowModelProperty.of(DataFlowModelProperty.Value.Pull));
    switch (commPattern) {
      case Shuffle:
        map.put(PartitionerProperty.of(PartitionerProperty.Value.HashPartitioner));
        map.put(InterTaskDataStoreProperty.of(InterTaskDataStoreProperty.Value.LocalFileStore));
        break;
      case BroadCast:
        map.put(PartitionerProperty.of(PartitionerProperty.Value.IntactPartitioner));
        map.put(InterTaskDataStoreProperty.of(InterTaskDataStoreProperty.Value.LocalFileStore));
        break;
      case OneToOne:
        map.put(PartitionerProperty.of(PartitionerProperty.Value.IntactPartitioner));
        map.put(InterTaskDataStoreProperty.of(InterTaskDataStoreProperty.Value.MemoryStore));
        break;
      default:
        map.put(PartitionerProperty.of(PartitionerProperty.Value.HashPartitioner));
        map.put(InterTaskDataStoreProperty.of(InterTaskDataStoreProperty.Value.LocalFileStore));
    }
    return map;
  }

  /**
   * Static initializer for irVertex.
   * @param irVertex irVertex to keep the execution property of.
   * @return The corresponding ExecutionPropertyMap.
   */
  public static ExecutionPropertyMap<VertexExecutionProperty> of(final IRVertex irVertex) {
    final ExecutionPropertyMap<VertexExecutionProperty> map = new ExecutionPropertyMap<>(irVertex.getId());
    map.put(ParallelismProperty.of(1));
    map.put(ExecutorPlacementProperty.of(ExecutorPlacementProperty.NONE));
    return map;
  }

  /**
   * ID of the item this ExecutionPropertyMap class is keeping track of.
   * @return the ID of the item this ExecutionPropertyMap class is keeping track of.
   */
  public String getId() {
    return id;
  }

  /**
   * Put the given execution property  in the ExecutionPropertyMap.
   * @param executionProperty execution property to insert.
   * @return the previous execution property, or null if there was no execution property with the specified property key
   */
  public T put(final T executionProperty) {
    return properties.put(executionProperty.getClass(), executionProperty);
  }

  /**
   * Get the value of the given execution property type.
   * @param <U> Type of the return value.
   * @param executionPropertyKey the execution property type to find the value of.
   * @return the value of the given execution property.
   */
  public <U extends Serializable> Optional<U> get(final Class<? extends ExecutionProperty<U>> executionPropertyKey) {
    final ExecutionProperty<U> property = properties.get(executionPropertyKey);
    return property == null ? Optional.empty() : Optional.of(property.getValue());
  }

  /**
   * remove the execution property.
   * @param key key of the execution property to remove.
   * @return the removed execution property
   */
  public T remove(final Class<? extends T> key) {
    return properties.remove(key);
  }

  /**
   * @param key key to look for.
   * @return whether or not the execution property map contains the key.
   */
  public boolean containsKey(final Class<? extends T> key) {
    return properties.containsKey(key);
  }

  /**
   * Same as forEach function in Java 8, but for execution properties.
   * @param action action to apply to each of the execution properties.
   */
  public void forEachProperties(final Consumer<? super T> action) {
    properties.values().forEach(action);
  }

  /**
   * @return {@link Stream} of execution properties.
   */
  public Stream<T> stream() {
    return properties.values().stream();
  }

  @Override
  public String toString() {
    final StringBuilder sb = new StringBuilder();
    sb.append("{");
    boolean isFirstPair = true;
    for (final Map.Entry<Class<? extends ExecutionProperty>, T> entry : properties.entrySet()) {
      if (!isFirstPair) {
        sb.append(", ");
      }
      isFirstPair = false;
      sb.append("\"");
      sb.append(entry.getKey().getCanonicalName());
      sb.append("\": \"");
      sb.append(entry.getValue().getValue());
      sb.append("\"");
    }
    sb.append("}");
    return sb.toString();
  }

  // Apache commons-lang 3 Equals/HashCodeBuilder template.
  @Override
  public boolean equals(final Object obj) {
    if (this == obj) {
      return true;
    }
    if (obj == null || getClass() != obj.getClass()) {
      return false;
    }
    final ExecutionPropertyMap that = (ExecutionPropertyMap) obj;
    return properties.values().stream().collect(Collectors.toSet())
        .equals(that.properties.values().stream().collect(Collectors.toSet()));
  }

  @Override
  public int hashCode() {
    return new HashCodeBuilder(17, 37)
        .append(properties.values().stream().map(ExecutionProperty::getValue).collect(Collectors.toSet()))
        .toHashCode();
  }
}
