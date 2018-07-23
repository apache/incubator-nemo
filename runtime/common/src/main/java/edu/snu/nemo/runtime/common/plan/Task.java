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
package edu.snu.nemo.runtime.common.plan;

import edu.snu.nemo.common.ir.Readable;
import edu.snu.nemo.common.ir.executionproperty.ExecutionPropertyMap;
import edu.snu.nemo.common.ir.executionproperty.VertexExecutionProperty;

import java.io.Serializable;
import java.util.*;

/**
 * A Task is a self-contained executable that can be executed on a machine.
 */
public final class Task implements Serializable {
  private final String jobId;
  private final String taskId;
  private final List<StageEdge> taskIncomingEdges;
  private final List<StageEdge> taskOutgoingEdges;
  private final int attemptIdx;
  private final ExecutionPropertyMap<VertexExecutionProperty> executionProperties;
  private final byte[] serializedIRDag;
  private final Map<String, Readable> irVertexIdToReadable;

  /**
   * Constructor.
   *
   * @param jobId                the id of the job.
   * @param taskId               the ID of the task.
   * @param attemptIdx           the attempt index.
   * @param executionProperties  {@link VertexExecutionProperty} map for the corresponding stage
   * @param serializedIRDag      the serialized DAG of the task.
   * @param taskIncomingEdges    the incoming edges of the task.
   * @param taskOutgoingEdges    the outgoing edges of the task.
   * @param irVertexIdToReadable the map between IRVertex id to readable.
   */
  public Task(final String jobId,
              final String taskId,
              final int attemptIdx,
              final ExecutionPropertyMap<VertexExecutionProperty> executionProperties,
              final byte[] serializedIRDag,
              final List<StageEdge> taskIncomingEdges,
              final List<StageEdge> taskOutgoingEdges,
              final Map<String, Readable> irVertexIdToReadable) {
    this.jobId = jobId;
    this.taskId = taskId;
    this.attemptIdx = attemptIdx;
    this.executionProperties = executionProperties;
    this.serializedIRDag = serializedIRDag;
    this.taskIncomingEdges = taskIncomingEdges;
    this.taskOutgoingEdges = taskOutgoingEdges;
    this.irVertexIdToReadable = irVertexIdToReadable;
  }

  public Task clone() {
    return new Task(jobId,
        taskId,
        attemptIdx,
        executionProperties.getCopy(),
        serializedIRDag,
        new ArrayList<>(taskIncomingEdges),
        new ArrayList<>(taskOutgoingEdges),
        new HashMap<>(irVertexIdToReadable));
  }

  /**
   * @return the id of the job.
   */
  public String getJobId() {
    return jobId;
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
    return attemptIdx;
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
    sb.append("jobId: ");
    sb.append(jobId);
    sb.append(" / taskId: ");
    sb.append(taskId);
    sb.append(" / attempt: ");
    sb.append(attemptIdx);
    sb.append(" / incoming: ");
    sb.append(taskIncomingEdges);
    sb.append(" / outgoing: ");
    sb.append(taskOutgoingEdges);
    sb.append("/ exec props: ");
    sb.append(getExecutionProperties());
    return sb.toString();
  }
}
