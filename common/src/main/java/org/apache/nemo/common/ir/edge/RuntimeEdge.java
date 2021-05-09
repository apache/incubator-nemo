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
package org.apache.nemo.common.ir.edge;

import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.nemo.common.dag.Edge;
import org.apache.nemo.common.dag.Vertex;
import org.apache.nemo.common.ir.edge.executionproperty.*;
import org.apache.nemo.common.ir.executionproperty.EdgeExecutionProperty;
import org.apache.nemo.common.ir.executionproperty.ExecutionPropertyMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.Serializable;
import java.util.List;
import java.util.Optional;

/**
 * Represents the edge between vertices in a logical/physical plan in runtime.
 * @param <V> the vertex type.
 */
public class RuntimeEdge<V extends Vertex> extends Edge<V> {
  private static final Logger LOG = LoggerFactory.getLogger(RuntimeEdge.class.getName());
  private final ExecutionPropertyMap<EdgeExecutionProperty> executionProperties;

  /**
   * Constructs the edge given the below parameters.
   *
   * @param runtimeEdgeId  the id of this edge.
   * @param executionProperties to control the data flow on this edge.
   * @param src            the source vertex.
   * @param dst            the destination vertex.
   */
  public RuntimeEdge(final String runtimeEdgeId,
                     final ExecutionPropertyMap<EdgeExecutionProperty> executionProperties,
                     final V src,
                     final V dst) {
    super(runtimeEdgeId, src, dst);
    this.executionProperties = executionProperties;
  }

  public boolean isTransientPath() {
    final CommunicationPatternProperty.Value val =
      executionProperties.get(CommunicationPatternProperty.class).get();

    LOG.info("Runtime edge {}, transient path check {}", getId(), val);

    return val.equals(CommunicationPatternProperty.Value.TransientOneToOne) ||
      val.equals(CommunicationPatternProperty.Value.TransientRR) ||
      val.equals(CommunicationPatternProperty.Value.TransientBroadcast) ||
      val.equals(CommunicationPatternProperty.Value.TransientShuffle);
  }

  public void removeEncoderDecoder() {
    executionProperties.remove(EncoderProperty.class);
    executionProperties.remove(DecoderProperty.class);
    executionProperties.remove(CompressionProperty.class);
    executionProperties.remove(DecompressionProperty.class);
  }

  private static <V extends Vertex> V getVertex(final String vertexId, final List<V> vertices) {
    for (final V v : vertices) {
      if (v.getId().equals(vertexId)) {
        return v;
      }
    }
    return null;
  }

  public static <V extends Vertex> RuntimeEdge decode(DataInputStream dis, List<V> vertices) {

    try {
      final String id = dis.readUTF();
      final ExecutionPropertyMap<EdgeExecutionProperty> p = ExecutionPropertyMap.decode(dis);
      final String srcId = dis.readUTF();
      final String dstId = dis.readUTF();
      final V src = getVertex(srcId, vertices);
      final V dst = getVertex(dstId, vertices);
      return new RuntimeEdge(id, p, src, dst);
    } catch (IOException e) {
      e.printStackTrace();
      throw new RuntimeException(e);
    }

  }

  public void encode(DataOutputStream dos) {
    try {
      dos.writeUTF(getId());
      executionProperties.encode(dos);
      dos.writeUTF(getSrc().getId());
      dos.writeUTF(getDst().getId());
    } catch (IOException e) {
      e.printStackTrace();
      throw new RuntimeException(e);
    }
  }

  /**
   * Get the execution property of the Runtime Edge.
   *
   * @param <T>                  Type of the return value.
   * @param executionPropertyKey key of the execution property.
   * @return the execution property.
   */
  public final <T extends Serializable> Optional<T> getPropertyValue(
      final Class<? extends EdgeExecutionProperty<T>> executionPropertyKey) {
    return executionProperties.get(executionPropertyKey);
  }

  /**
   * @param executionPropertyKey key
   * @param <T> type
   * @return the value
   */
  public final <T extends Serializable> T getPropertyValueOrRuntimeException(
    final Class<? extends EdgeExecutionProperty<T>> executionPropertyKey) {
    final Optional<T> optional = getPropertyValue(executionPropertyKey);
    if (optional.isPresent()) {
      return optional.get();
    } else {
      throw new IllegalStateException(executionPropertyKey.toString());
    }
  }

  /**
   * @return the ExecutionPropertyMap of the Runtime Edge.
   */
  public final ExecutionPropertyMap<EdgeExecutionProperty> getExecutionProperties() {
    return executionProperties;
  }

  @Override
  public String toString() {
    return "[" + getId() + ": " + getSrc() + "->" + getDst() + "]";
  }

  /**
   * @return JSON representation of additional properties
   */
  @Override
  @SuppressWarnings("checkstyle:designforextension")
  public ObjectNode getPropertiesAsJsonNode() {
    final ObjectNode node = JsonNodeFactory.instance.objectNode();
    node.put("runtimeEdgeId", getId());
    node.set("executionProperties", executionProperties.asJsonNode());
    return node;
  }
}
