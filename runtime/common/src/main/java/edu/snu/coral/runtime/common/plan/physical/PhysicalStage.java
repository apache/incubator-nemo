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

import edu.snu.coral.common.dag.DAG;
import edu.snu.coral.common.dag.Vertex;
import edu.snu.coral.common.ir.Readable;
import edu.snu.coral.runtime.common.RuntimeIdGenerator;
import edu.snu.coral.runtime.common.plan.RuntimeEdge;
import org.apache.commons.lang3.SerializationUtils;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * PhysicalStage.
 */
public final class PhysicalStage extends Vertex {
  private final DAG<Task, RuntimeEdge<Task>> taskGroupDag;
  private final int parallelism;
  private final int scheduleGroupIndex;
  private final String containerType;
  private final byte[] serializedTaskGroupDag;
  private final List<Map<String, Readable>> logicalTaskIdToReadables;

  /**
   * Constructor.
   *
   * @param stageId                  ID of the stage.
   * @param taskGroupDag             the DAG of the task group in this stage.
   * @param parallelism              how many task groups will be executed in this stage.
   * @param scheduleGroupIndex       the schedule group index.
   * @param containerType            the type of container to execute the task group on.
   * @param logicalTaskIdToReadables the list of maps between logical task ID and {@link Readable}.
   */
  public PhysicalStage(final String stageId,
                       final DAG<Task, RuntimeEdge<Task>> taskGroupDag,
                       final int parallelism,
                       final int scheduleGroupIndex,
                       final String containerType,
                       final List<Map<String, Readable>> logicalTaskIdToReadables) {
    super(stageId);
    this.taskGroupDag = taskGroupDag;
    this.parallelism = parallelism;
    this.scheduleGroupIndex = scheduleGroupIndex;
    this.containerType = containerType;
    this.serializedTaskGroupDag = SerializationUtils.serialize(taskGroupDag);
    this.logicalTaskIdToReadables = logicalTaskIdToReadables;
  }

  /**
   * @return the task group.
   */
  public DAG<Task, RuntimeEdge<Task>> getTaskGroupDag() {
    return taskGroupDag;
  }

  /**
   * @return the serialized DAG of the task group.
   */
  public byte[] getSerializedTaskGroupDag() {
    return serializedTaskGroupDag;
  }

  /**
   * @return the list of the task group IDs in this stage.
   */
  public List<String> getTaskGroupIds() {
    final List<String> taskGroupIds = new ArrayList<>();
    for (int taskGroupIdx = 0; taskGroupIdx < parallelism; taskGroupIdx++) {
      taskGroupIds.add(RuntimeIdGenerator.generateTaskGroupId(taskGroupIdx, getId()));
    }
    return taskGroupIds;
  }

  /**
   * @return the schedule group index.
   */
  public int getScheduleGroupIndex() {
    return scheduleGroupIndex;
  }

  /**
   * @return the type of container to execute the task group on.
   */
  public String getContainerType() {
    return containerType;
  }

  /**
   * @return the list of maps between logical task ID and readable.
   */
  public List<Map<String, Readable>> getLogicalTaskIdToReadables() {
    return logicalTaskIdToReadables;
  }

  @Override
  public String propertiesToJSON() {
    final StringBuilder sb = new StringBuilder();
    sb.append("{\"scheduleGroupIndex\": ").append(scheduleGroupIndex);
    sb.append(", \"taskGroupDag\": ").append(taskGroupDag);
    sb.append(", \"parallelism\": ").append(parallelism);
    sb.append(", \"containerType\": \"").append(containerType).append("\"");
    sb.append('}');
    return sb.toString();
  }
}
