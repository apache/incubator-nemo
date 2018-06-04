/*
 * Copyright (C) 2017 Seoul National University
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
import edu.snu.nemo.runtime.common.RuntimeIdGenerator;

import java.io.Serializable;
import java.util.List;
import java.util.Map;

/**
 * An ExecutableTask is a self-contained executable that can be executed in specific types of containers.
 */
public final class ExecutableTask implements Serializable {
  private final String jobId;
  private final String taskId;
  private final int taskIdx;
  private final List<StageEdge> taskIncomingEdges;
  private final List<StageEdge> taskOutgoingEdges;
  private final int attemptIdx;
  private final String containerType;
  private final byte[] serializedTaskDag;
  private final Map<String, Readable> irVertexIdToReadable;

  /**
   * Constructor.
   *
   * @param jobId                the id of the job.
   * @param taskId               the ID of the scheduled task.
   * @param attemptIdx           the attempt index.
   * @param containerType        the type of container to execute the task on.
   * @param serializedIRDag      the serialized DAG of the task.
   * @param taskIncomingEdges    the incoming edges of the task.
   * @param taskOutgoingEdges    the outgoing edges of the task.
   * @param irVertexIdToReadable the map between logical task ID and readable.
   */
  public ExecutableTask(final String jobId,
                        final String taskId,
                        final int attemptIdx,
                        final String containerType,
                        final byte[] serializedIRDag,
                        final List<StageEdge> taskIncomingEdges,
                        final List<StageEdge> taskOutgoingEdges,
                        final Map<String, Readable> irVertexIdToReadable) {
    this.jobId = jobId;
    this.taskId = taskId;
    this.taskIdx = RuntimeIdGenerator.getIndexFromTaskId(taskId);
    this.attemptIdx = attemptIdx;
    this.containerType = containerType;
    this.serializedTaskDag = serializedIRDag;
    this.taskIncomingEdges = taskIncomingEdges;
    this.taskOutgoingEdges = taskOutgoingEdges;
    this.irVertexIdToReadable = irVertexIdToReadable;
  }

  /**
   * @return the id of the job.
   */
  public String getJobId() {
    return jobId;
  }

  /**
   * @return the serialized DAG of the task.
   */
  public byte[] getSerializedIRDag() {
    return serializedTaskDag;
  }

  /**
   * @return the ID of the scheduled task.
   */
  public String getTaskId() {
    return taskId;
  }

  /**
   * @return the idx of the scheduled task.
   */
  public int getTaskIdx() {
    return taskIdx;
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
   * @return the type of container to execute the task on.
   */
  public String getContainerType() {
    return containerType;
  }

  /**
   * @return the map between logical task ID and readable.
   */
  public Map<String, Readable> getIrVertexIdToReadable() {
    return irVertexIdToReadable;
  }
}
