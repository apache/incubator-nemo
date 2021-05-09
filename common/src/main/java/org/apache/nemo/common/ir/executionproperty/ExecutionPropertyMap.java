/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.nemo.common.ir.executionproperty;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.commons.lang3.SerializationUtils;
import org.apache.nemo.common.coder.DecoderFactory;
import org.apache.nemo.common.coder.EncoderFactory;
import org.apache.nemo.common.exception.CompileTimeOptimizationException;
import org.apache.nemo.common.ir.edge.IREdge;
import org.apache.nemo.common.ir.edge.executionproperty.*;
import org.apache.nemo.common.ir.vertex.IRVertex;
import org.apache.nemo.common.ir.vertex.executionproperty.ResourcePriorityProperty;

import com.google.common.annotations.VisibleForTesting;
import org.apache.commons.lang3.builder.HashCodeBuilder;

import javax.annotation.concurrent.NotThreadSafe;
import java.io.DataInputStream;
import java.io.DataOutputStream;
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
  private final Map<Class<? extends ExecutionProperty>, T> properties;
  private final Set<Class<? extends ExecutionProperty>> finalizedProperties;

  private ExecutionPropertyMap(final String id,
                               final Map<Class<? extends ExecutionProperty>, T> properties,
                               final Set<Class<? extends ExecutionProperty>> finalizedProperties) {
    this.id = id;
    this.properties = properties;
    this.finalizedProperties = finalizedProperties;
  }

  public void encode(final DataOutputStream os) {
    try {
      os.writeUTF(id);
      os.writeInt(properties.size());
      properties.forEach((key, val) -> {
        SerializationUtils.serialize(key, os);
        SerializationUtils.serialize(val, os);
      });

      os.writeInt(finalizedProperties.size());
      finalizedProperties.forEach(val -> {
        SerializationUtils.serialize(val, os);
      });

    } catch (final Exception e) {
      e.printStackTrace();
      throw new RuntimeException(e);
    }
  }

  public static <T> ExecutionPropertyMap decode(final DataInputStream dis) {
     try {

       final String id = dis.readUTF();
       final int psize = dis.readInt();
       final Map<Class<? extends ExecutionProperty>, T> properties = new HashMap<>(psize);
       for (int i = 0; i < psize; i++) {
         final Class<? extends ExecutionProperty> key = SerializationUtils.deserialize(dis);
         final T val = SerializationUtils.deserialize(dis);
         properties.put(key, val);
       }

       final Set<Class<? extends ExecutionProperty>> finalizedProperties = new HashSet<>();
       final int fsize = dis.readInt();
       for (int i = 0; i < fsize; i++) {
         final Class<? extends ExecutionProperty> val = SerializationUtils.deserialize(dis);
         finalizedProperties.add(val);
       }

       return new ExecutionPropertyMap(id, properties, finalizedProperties);

    } catch (final Exception e) {
      e.printStackTrace();
      throw new RuntimeException(e);
    }
  }

  /**
   * Constructor for ExecutionPropertyMap class.
   * @param id ID of the vertex / edge to keep the execution property of.
   */
  @VisibleForTesting
  public ExecutionPropertyMap(final String id) {
    this.id = id;
    this.properties = new HashMap<>();
    this.finalizedProperties = new HashSet<>();
  }

  /**
   * Static initializer for irEdges.
   * @param irEdge irEdge to keep the execution property of.
   * @param commPattern Data communication pattern type of the edge.
   * @return The corresponding ExecutionPropertyMap.
   */
  public static ExecutionPropertyMap<EdgeExecutionProperty> of(
      final IREdge irEdge,
      final CommunicationPatternProperty.Value commPattern) {
    final ExecutionPropertyMap<EdgeExecutionProperty> map = new ExecutionPropertyMap<>(irEdge.getId());
    map.put(CommunicationPatternProperty.of(commPattern));
    map.put(EncoderProperty.of(EncoderFactory.DUMMY_ENCODER_FACTORY));
    map.put(DecoderProperty.of(DecoderFactory.DUMMY_DECODER_FACTORY));
    switch (commPattern) {
      case RoundRobin:
      case TransientRR:
      case Shuffle:
      case TransientShuffle:
        map.put(DataFlowProperty.of(DataFlowProperty.Value.Pull));
        map.put(PartitionerProperty.of(PartitionerProperty.Type.Hash));
        map.put(DataStoreProperty.of(DataStoreProperty.Value.LocalFileStore));
        break;
      case BroadCast:
      case TransientBroadcast:
        map.put(DataFlowProperty.of(DataFlowProperty.Value.Pull));
        map.put(PartitionerProperty.of(PartitionerProperty.Type.Intact));
        map.put(DataStoreProperty.of(DataStoreProperty.Value.LocalFileStore));
        break;
      case OneToOne:
      case TransientOneToOne:
        map.put(DataFlowProperty.of(DataFlowProperty.Value.Push));
        map.put(PartitionerProperty.of(PartitionerProperty.Type.Intact));
        map.put(DataStoreProperty.of(DataStoreProperty.Value.MemoryStore));
        break;
      default:
        throw new IllegalStateException(commPattern.toString());
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
    map.put(ResourcePriorityProperty.of(ResourcePriorityProperty.NONE));
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
   * Put the given execution property  in the ExecutionPropertyMap. By default, it does not finalize the property.
   * @param executionProperty execution property to insert.
   * @return the previous execution property, or null if there was no execution property
   * with the specified property key.
   */
  public T put(final T executionProperty) {
    return this.put(executionProperty, false);
  }

  /**
   * Put the given execution property in the ExecutionPropertyMap.
   * @param executionProperty execution property to insert.
   * @param finalize whether or not to finalize the execution property.
   * @return the previous execution property, or null if there was no execution property
   * with the specified property key.
   */
  public T put(final T executionProperty, final Boolean finalize) {
    // check if the property has been already finalized. We don't mind overwriting an identical value.
    if (finalizedProperties.contains(executionProperty.getClass())
        && properties.get(executionProperty.getClass()) != null
        && !properties.get(executionProperty.getClass()).equals(executionProperty)) {
      throw new CompileTimeOptimizationException("Trying to overwrite a finalized execution property ["
          + executionProperty.getClass().getSimpleName() + "] from ["
          + properties.get(executionProperty.getClass()).getValue() + "] to [" + executionProperty.getValue() + "]");
    }

    // start the actual put process.
    if (finalize) {
      this.finalizedProperties.add(executionProperty.getClass());
    }
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
    return asJsonNode().toString();
  }

  /**
   * @return {@link com.fasterxml.jackson.databind.JsonNode} for this execution property map.
   */
  public ObjectNode asJsonNode() {
    final ObjectMapper mapper = new ObjectMapper();
    final ObjectNode node = mapper.createObjectNode();
    for (final Map.Entry<Class<? extends ExecutionProperty>, T> entry : properties.entrySet()) {
      node.put(entry.getKey().getCanonicalName(), entry.getValue().getValue().toString());
    }
    return node;
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
