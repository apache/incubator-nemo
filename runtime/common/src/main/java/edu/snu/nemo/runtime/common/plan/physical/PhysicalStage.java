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

import edu.snu.nemo.common.dag.DAG;
import edu.snu.nemo.common.dag.Vertex;
import edu.snu.nemo.common.ir.Readable;
import edu.snu.nemo.runtime.common.RuntimeIdGenerator;
import edu.snu.nemo.runtime.common.plan.RuntimeEdge;
import org.apache.commons.lang3.SerializationUtils;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * PhysicalStage.
 */
public final class PhysicalStage extends Vertex {
  private final DAG<Task, RuntimeEdge<Task>> taskDag;
  private final int parallelism;
  private final int scheduleGroupIndex;
  private final String containerType;
  private final byte[] serializedTaskDag;
  private final List<Map<String, Readable>> logicalTaskIdToReadables;

  /**
   * Constructor.
   *
   * @param stageId                  ID of the stage.
   * @param taskDag             the DAG of the task in this stage.
   * @param parallelism              how many tasks will be executed in this stage.
   * @param scheduleGroupIndex       the schedule group index.
   * @param containerType            the type of container to execute the task on.
   * @param logicalTaskIdToReadables the list of maps between logical task ID and {@link Readable}.
   */
  public PhysicalStage(final String stageId,
                       final DAG<Task, RuntimeEdge<Task>> taskDag,
                       final int parallelism,
                       final int scheduleGroupIndex,
                       final String containerType,
                       final List<Map<String, Readable>> logicalTaskIdToReadables) {
    super(stageId);
    this.taskDag = taskDag;
    this.parallelism = parallelism;
    this.scheduleGroupIndex = scheduleGroupIndex;
    this.containerType = containerType;
    this.serializedTaskDag = SerializationUtils.serialize(taskDag);
    this.logicalTaskIdToReadables = logicalTaskIdToReadables;
  }

  /**
   * @return the task.
   */
  public DAG<Task, RuntimeEdge<Task>> getTaskDag() {
    return taskDag;
  }

  /**
   * @return the serialized DAG of the task.
   */
  public byte[] getSerializedTaskDag() {
    return serializedTaskDag;
  }

  /**
   * @return the list of the task IDs in this stage.
   */
  public List<String> getTaskIds() {
    final List<String> taskIds = new ArrayList<>();
    for (int taskIdx = 0; taskIdx < parallelism; taskIdx++) {
      taskIds.add(RuntimeIdGenerator.generateTaskId(taskIdx, getId()));
    }
    return taskIds;
  }

  /**
   * @return the schedule group index.
   */
  public int getScheduleGroupIndex() {
    return scheduleGroupIndex;
  }

  /**
   * @return the type of container to execute the task on.
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
    sb.append(", \"taskDag\": ").append(taskDag);
    sb.append(", \"parallelism\": ").append(parallelism);
    sb.append(", \"containerType\": \"").append(containerType).append("\"");
    sb.append('}');
    return sb.toString();
  }
}
