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
package edu.snu.onyx.runtime.common.plan.physical;

import edu.snu.onyx.runtime.common.RuntimeIdGenerator;

import java.io.Serializable;
import java.util.List;

/**
 * A wrapper for TaskGroup.
 * This class includes the incoming/outgoing edges to/from the stage the TaskGroup belongs to.
 * The information is required in Executors to resolve intermediate data dependencies.
 *
 * Thus, this class is serialized and sent to executors by the scheduler (instead of TaskGroup).
 */
public final class ScheduledTaskGroup implements Serializable {
  private final String jobId;
  private final TaskGroup taskGroup;
  private final String taskGroupId;
  private final int taskGroupIdx;
  private final List<PhysicalStageEdge> taskGroupIncomingEdges;
  private final List<PhysicalStageEdge> taskGroupOutgoingEdges;
  private final int attemptIdx;

  /**
   * Constructor.
   *
   * @param jobId                  the id of the job.
   * @param taskGroup              the scheduled task group.
   * @param taskGroupId            the ID of the scheduled task group.
   * @param taskGroupIncomingEdges the incoming edges of the task group.
   * @param taskGroupOutgoingEdges the outgoing edges of the task group.
   * @param attemptIdx             the attempt index.
   */
  public ScheduledTaskGroup(final String jobId,
                            final TaskGroup taskGroup,
                            final String taskGroupId,
                            final List<PhysicalStageEdge> taskGroupIncomingEdges,
                            final List<PhysicalStageEdge> taskGroupOutgoingEdges,
                            final int attemptIdx) {
    this.jobId = jobId;
    this.taskGroup = taskGroup;
    this.taskGroupId = taskGroupId;
    this.taskGroupIdx = RuntimeIdGenerator.getIndexFromTaskGroupId(taskGroupId);
    this.taskGroupIncomingEdges = taskGroupIncomingEdges;
    this.taskGroupOutgoingEdges = taskGroupOutgoingEdges;
    this.attemptIdx = attemptIdx;
  }

  /**
   * @return the id of the job.
   */
  public String getJobId() {
    return jobId;
  }

  /**
   * @return the taskGroup
   */
  public TaskGroup getTaskGroup() {
    return taskGroup;
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
}
