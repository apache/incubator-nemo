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

import edu.snu.nemo.common.dag.DAG;
import edu.snu.nemo.common.dag.Vertex;
import edu.snu.nemo.common.ir.Readable;
import edu.snu.nemo.common.ir.vertex.IRVertex;
import edu.snu.nemo.runtime.common.RuntimeIdGenerator;
import org.apache.commons.lang3.SerializationUtils;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Stage.
 */
public final class Stage extends Vertex {
  private final DAG<IRVertex, RuntimeEdge<IRVertex>> irDag;
  private final int parallelism;
  private final int scheduleGroupIndex;
  private final String containerType;
  private final byte[] serializedIRDag;
  private final List<Map<String, Readable>> vertexIdToReadables;

  /**
   * Constructor.
   *
   * @param stageId             ID of the stage.
   * @param irDag               the DAG of the task in this stage.
   * @param parallelism         how many tasks will be executed in this stage.
   * @param scheduleGroupIndex  the schedule group index.
   * @param containerType       the type of container to execute the task on.
   * @param vertexIdToReadables the list of maps between vertex ID and {@link Readable}.
   */
  public Stage(final String stageId,
               final DAG<IRVertex, RuntimeEdge<IRVertex>> irDag,
               final int parallelism,
               final int scheduleGroupIndex,
               final String containerType,
               final List<Map<String, Readable>> vertexIdToReadables) {
    super(stageId);
    this.irDag = irDag;
    this.parallelism = parallelism;
    this.scheduleGroupIndex = scheduleGroupIndex;
    this.containerType = containerType;
    this.serializedIRDag = SerializationUtils.serialize(irDag);
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
   * @return the list of maps between vertex ID and readables.
   */
  public List<Map<String, Readable>> getVertexIdToReadables() {
    return vertexIdToReadables;
  }

  @Override
  public String propertiesToJSON() {
    final StringBuilder sb = new StringBuilder();
    sb.append("{\"scheduleGroupIndex\": ").append(scheduleGroupIndex);
    sb.append(", \"irDag\": ").append(irDag);
    sb.append(", \"parallelism\": ").append(parallelism);
    sb.append(", \"containerType\": \"").append(containerType).append("\"");
    sb.append('}');
    return sb.toString();
  }
}
