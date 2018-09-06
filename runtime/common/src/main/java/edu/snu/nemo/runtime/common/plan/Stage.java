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
package org.apache.nemo.runtime.common.plan;

import org.apache.nemo.common.dag.DAG;
import org.apache.nemo.common.dag.Vertex;
import org.apache.nemo.common.ir.Readable;
import org.apache.nemo.common.ir.executionproperty.ExecutionPropertyMap;
import org.apache.nemo.common.ir.executionproperty.VertexExecutionProperty;
import org.apache.nemo.common.ir.vertex.IRVertex;
import org.apache.nemo.common.ir.vertex.executionproperty.ParallelismProperty;
import org.apache.nemo.common.ir.vertex.executionproperty.ScheduleGroupProperty;
import org.apache.commons.lang3.SerializationUtils;

import java.io.Serializable;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * Stage.
 */
public final class Stage extends Vertex {
  private final DAG<IRVertex, RuntimeEdge<IRVertex>> irDag;
  private final byte[] serializedIRDag;
  private final ExecutionPropertyMap<VertexExecutionProperty> executionProperties;
  private final List<Map<String, Readable>> vertexIdToReadables;

  /**
   * Constructor.
   *
   * @param stageId             ID of the stage.
   * @param irDag               the DAG of the task in this stage.
   * @param executionProperties set of {@link VertexExecutionProperty} for this stage
   * @param vertexIdToReadables the list of maps between vertex ID and {@link Readable}.
   */
  public Stage(final String stageId,
               final DAG<IRVertex, RuntimeEdge<IRVertex>> irDag,
               final ExecutionPropertyMap<VertexExecutionProperty> executionProperties,
               final List<Map<String, Readable>> vertexIdToReadables) {
    super(stageId);
    this.irDag = irDag;
    this.serializedIRDag = SerializationUtils.serialize(irDag);
    this.executionProperties = executionProperties;
    this.vertexIdToReadables = vertexIdToReadables;
  }

  /**
   * @return the IRVertex DAG.
   */
  public DAG<IRVertex, RuntimeEdge<IRVertex>> getIRDAG() {
    return irDag;
  }

  /**
   * @return the serialized DAG of the task.
   */
  public byte[] getSerializedIRDAG() {
    return serializedIRDag;
  }

  /**
   * @return the parallelism
   */
  public int getParallelism() {
    return executionProperties.get(ParallelismProperty.class)
        .orElseThrow(() -> new RuntimeException("Parallelism property must be set for Stage"));
  }

  /**
   * @return the schedule group.
   */
  public int getScheduleGroup() {
    return executionProperties.get(ScheduleGroupProperty.class)
        .orElseThrow(() -> new RuntimeException("ScheduleGroup property must be set for Stage"));
  }

  /**
   * @return {@link VertexExecutionProperty} map for this stage
   */
  public ExecutionPropertyMap<VertexExecutionProperty> getExecutionProperties() {
    return executionProperties;
  }

  /**
   * Get the executionProperty of the IREdge.
   *
   * @param <T>                  Type of the return value.
   * @param executionPropertyKey key of the execution property.
   * @return the execution property.
   */
  public <T extends Serializable> Optional<T> getPropertyValue(
      final Class<? extends VertexExecutionProperty<T>> executionPropertyKey) {
    return executionProperties.get(executionPropertyKey);
  }

  /**
   * @return the list of maps between vertex ID and readables.
   */
  public List<Map<String, Readable>> getVertexIdToReadables() {
    return vertexIdToReadables;
  }

  @Override
  public String propertiesToJSON() {
    final StringBuilder sb = new StringBuilder();
    sb.append("{\"scheduleGroup\": ").append(getScheduleGroup());
    sb.append(", \"irDag\": ").append(irDag);
    sb.append(", \"parallelism\": ").append(getParallelism());
    sb.append(", \"executionProperties\": ").append(executionProperties);
    sb.append('}');
    return sb.toString();
  }
}
