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
package edu.snu.coral.runtime.common.plan.physical;

import edu.snu.coral.runtime.common.RuntimeIdGenerator;

import java.io.Serializable;
import java.util.List;

/**
 * A ScheduledTaskGroup is a grouping of {@link Task} that belong to a stage.
 * Executors receive units of ScheduledTaskGroups during job execution,
 * and thus the resource type of all tasks of a ScheduledTaskGroup must be identical.
 * A stage contains a list of IDs of TaskGroups whose length corresponds to stage/operator parallelism.
 *
 * This class includes all information which will be required from the executor after scheduled,
 * including the (serialized) DAG of {@link Task}s,
 * the incoming/outgoing edges to/from the stage the TaskGroup belongs to, and so on.
 */
public final class ScheduledTaskGroup implements Serializable {
  private final String jobId;
  private final String taskGroupId;
  private final int taskGroupIdx;
  private final List<PhysicalStageEdge> taskGroupIncomingEdges;
  private final List<PhysicalStageEdge> taskGroupOutgoingEdges;
  private final int attemptIdx;
  private final String containerType;
  private final byte[] serializedTaskGroupDag;
  private final boolean isSmall;

  /**
   * Constructor.
   *
   * @param jobId                  the id of the job.
   * @param serializedTaskGroupDag the serialized DAG of the task group.
   * @param taskGroupId            the ID of the scheduled task group.
   * @param taskGroupIncomingEdges the incoming edges of the task group.
   * @param taskGroupOutgoingEdges the outgoing edges of the task group.
   * @param attemptIdx             the attempt index.
   * @param containerType          the type of container to execute the task group on.
   * @param isSmall                whether this task group is small or not (scheduler hack for sailfish exp).
   */
  public ScheduledTaskGroup(final String jobId,
                            final byte[] serializedTaskGroupDag,
                            final String taskGroupId,
                            final List<PhysicalStageEdge> taskGroupIncomingEdges,
                            final List<PhysicalStageEdge> taskGroupOutgoingEdges,
                            final int attemptIdx,
                            final String containerType,
                            final boolean isSmall) {
    this.jobId = jobId;
    this.taskGroupId = taskGroupId;
    this.taskGroupIdx = RuntimeIdGenerator.getIndexFromTaskGroupId(taskGroupId);
    this.taskGroupIncomingEdges = taskGroupIncomingEdges;
    this.taskGroupOutgoingEdges = taskGroupOutgoingEdges;
    this.attemptIdx = attemptIdx;
    this.containerType = containerType;
    this.serializedTaskGroupDag = serializedTaskGroupDag;
    this.isSmall = isSmall;
  }

  /**
   * @return the id of the job.
   */
  public String getJobId() {
    return jobId;
  }

  /**
   * @return the serialized DAG of the task group.
   */
  public byte[] getSerializedTaskGroupDag() {
    return serializedTaskGroupDag;
  }

  /**
   * @return the ID of the scheduled task group.
   */
  public String getTaskGroupId() {
    return taskGroupId;
  }

  /**
   * @return the idx of the scheduled task group.
   */
  public int getTaskGroupIdx() {
    return taskGroupIdx;
  }

  /**
   * @return the incoming edges of the taskGroup.
   */
  public List<PhysicalStageEdge> getTaskGroupIncomingEdges() {
    return taskGroupIncomingEdges;
  }

  /**
   * @return the outgoing edges of the taskGroup.
   */
  public List<PhysicalStageEdge> getTaskGroupOutgoingEdges() {
    return taskGroupOutgoingEdges;
  }

  /**
   * @return the attempt index.
   */
  public int getAttemptIdx() {
    return attemptIdx;
  }

  /**
   * @return the type of container to execute the task group on.
   */
  public String getContainerType() {
    return containerType;
  }

  public boolean isSmall() {
    return isSmall;
  }
}
