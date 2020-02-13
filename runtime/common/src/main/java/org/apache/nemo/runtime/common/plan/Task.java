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
package org.apache.nemo.runtime.common.plan;

import org.apache.nemo.common.ir.Readable;
import org.apache.nemo.common.ir.executionproperty.ExecutionPropertyMap;
import org.apache.nemo.common.ir.executionproperty.VertexExecutionProperty;
import org.apache.nemo.runtime.common.RuntimeIdManager;

import java.io.Serializable;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * A Task (attempt) is a self-contained executable that can be executed on a machine.
 */
public final class Task implements Serializable {
  private final String planId;
  private final String taskId;
  private final List<StageEdge> taskIncomingEdges;
  private final List<StageEdge> taskOutgoingEdges;
  private final ExecutionPropertyMap<VertexExecutionProperty> executionProperties;
  private final byte[] serializedIRDag;
  private final Map<String, Readable> irVertexIdToReadable;

  /**
   * Constructor.
   *
   * @param planId               the id of the physical plan.
   * @param taskId               the ID of this task attempt.
   * @param executionProperties  {@link VertexExecutionProperty} map for the corresponding stage
   * @param serializedIRDag      the serialized DAG of the task.
   * @param taskIncomingEdges    the incoming edges of the task.
   * @param taskOutgoingEdges    the outgoing edges of the task.
   * @param irVertexIdToReadable the map between IRVertex id to readable.
   */
  public Task(final String planId,
              final String taskId,
              final ExecutionPropertyMap<VertexExecutionProperty> executionProperties,
              final byte[] serializedIRDag,
              final List<StageEdge> taskIncomingEdges,
              final List<StageEdge> taskOutgoingEdges,
              final Map<String, Readable> irVertexIdToReadable) {
    this.planId = planId;
    this.taskId = taskId;
    this.executionProperties = executionProperties;
    this.serializedIRDag = serializedIRDag;
    this.taskIncomingEdges = taskIncomingEdges;
    this.taskOutgoingEdges = taskOutgoingEdges;
    this.irVertexIdToReadable = irVertexIdToReadable;
  }

  /**
   * @return the id of the plan.
   */
  public String getPlanId() {
    return planId;
  }

  /**
   * @return the serialized IR DAG of the task.
   */
  public byte[] getSerializedIRDag() {
    return serializedIRDag;
  }

  /**
   * @return the ID of the task.
   */
  public String getTaskId() {
    return taskId;
  }

  /**
   * @return the Stage ID of the task.
   */
  public String getStageId() {
    return RuntimeIdManager.getStageIdFromTaskId(this.getTaskId());
  }

  /**
   * @return the incoming edges of the task.
   */
  public List<StageEdge> getTaskIncomingEdges() {
    return taskIncomingEdges;
  }

  /**
   * @return the outgoing edges of the task.
   */
  public List<StageEdge> getTaskOutgoingEdges() {
    return taskOutgoingEdges;
  }

  /**
   * @return the attempt index.
   */
  public int getAttemptIdx() {
    return RuntimeIdManager.getAttemptFromTaskId(taskId);
  }

  /**
   * @return {@link VertexExecutionProperty} map for the corresponding stage
   */
  public ExecutionPropertyMap<VertexExecutionProperty> getExecutionProperties() {
    return executionProperties;
  }

  /**
   * Get the executionProperty of this task.
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
   * @return the map between IRVertex id and readable.
   */
  public Map<String, Readable> getIrVertexIdToReadable() {
    return irVertexIdToReadable;
  }

  @Override
  public String toString() {
    final StringBuilder sb = new StringBuilder();
    sb.append("planId: ");
    sb.append(planId);
    sb.append(" / taskId: ");
    sb.append(taskId);
    sb.append(" / attempt: ");
    sb.append(getAttemptIdx());
    sb.append(" / incoming: ");
    sb.append(taskIncomingEdges);
    sb.append(" / outgoing: ");
    sb.append(taskOutgoingEdges);
    sb.append("/ exec props: ");
    sb.append(getExecutionProperties());
    return sb.toString();
  }
}
