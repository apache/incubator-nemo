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
package edu.snu.nemo.runtime.common.plan.physical;

import edu.snu.nemo.common.ir.Readable;
import edu.snu.nemo.runtime.common.RuntimeIdGenerator;

import java.io.Serializable;
import java.util.List;
import java.util.Map;

/**
 * A ScheduledTask is a grouping of {@link Task}s that belong to a stage.
 * Executors receive units of ScheduledTasks during job execution,
 * and thus the resource type of all tasks of a ScheduledTask must be identical.
 * A stage contains a list of IDs of Tasks whose length corresponds to stage/operator parallelism.
 *
 * This class includes all information which will be required from the executor after scheduled,
 * including the (serialized) DAG of {@link Task}s,
 * the incoming/outgoing edges to/from the stage the Task belongs to, and so on.
 */
public final class ScheduledTask implements Serializable {
  private final String jobId;
  private final String taskId;
  private final int taskIdx;
  private final List<PhysicalStageEdge> taskIncomingEdges;
  private final List<PhysicalStageEdge> taskOutgoingEdges;
  private final int attemptIdx;
  private final String containerType;
  private final byte[] serializedTaskDag;
  private final Map<String, Readable> logicalTaskIdToReadable;

  /**
   * Constructor.
   *
   * @param jobId                   the id of the job.
   * @param serializedTaskDag  the serialized DAG of the task.
   * @param taskId             the ID of the scheduled task.
   * @param taskIncomingEdges  the incoming edges of the task.
   * @param taskOutgoingEdges  the outgoing edges of the task.
   * @param attemptIdx              the attempt index.
   * @param containerType           the type of container to execute the task on.
   * @param logicalTaskIdToReadable the map between logical task ID and readable.
   */
  public ScheduledTask(final String jobId,
                            final byte[] serializedTaskDag,
                            final String taskId,
                            final List<PhysicalStageEdge> taskIncomingEdges,
                            final List<PhysicalStageEdge> taskOutgoingEdges,
                            final int attemptIdx,
                            final String containerType,
                            final Map<String, Readable> logicalTaskIdToReadable) {
    this.jobId = jobId;
    this.taskId = taskId;
    this.taskIdx = RuntimeIdGenerator.getIndexFromTaskId(taskId);
    this.taskIncomingEdges = taskIncomingEdges;
    this.taskOutgoingEdges = taskOutgoingEdges;
    this.attemptIdx = attemptIdx;
    this.containerType = containerType;
    this.serializedTaskDag = serializedTaskDag;
    this.logicalTaskIdToReadable = logicalTaskIdToReadable;
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
  public byte[] getSerializedTaskDag() {
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
  public List<PhysicalStageEdge> getTaskIncomingEdges() {
    return taskIncomingEdges;
  }

  /**
   * @return the outgoing edges of the task.
   */
  public List<PhysicalStageEdge> getTaskOutgoingEdges() {
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
  public Map<String, Readable> getLogicalTaskIdToReadable() {
    return logicalTaskIdToReadable;
  }
}
