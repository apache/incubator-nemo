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
package org.apache.nemo.common;

import org.apache.commons.lang3.SerializationUtils;
import org.apache.nemo.common.dag.DAG;
import org.apache.nemo.common.exception.UnsupportedCommPatternException;
import org.apache.nemo.common.ir.Readable;
import org.apache.nemo.common.ir.edge.RuntimeEdge;
import org.apache.nemo.common.ir.edge.StageEdge;
import org.apache.nemo.common.ir.edge.executionproperty.CommunicationPatternProperty;
import org.apache.nemo.common.ir.executionproperty.ExecutionPropertyMap;
import org.apache.nemo.common.ir.executionproperty.VertexExecutionProperty;
import org.apache.nemo.common.ir.vertex.IRVertex;
import org.apache.nemo.common.ir.vertex.executionproperty.ParallelismProperty;
import org.apache.nemo.common.ir.vertex.utility.StateMergerVertex;
import org.apache.nemo.common.ir.vertex.utility.StreamVertex;
import org.apache.nemo.offloading.common.TaskCaching;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.Serializable;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/**
 * A Task (attempt) is a self-contained executable that can be executed on a machine.
 */
public final class Task implements Serializable {
  private static final Logger LOG = LoggerFactory.getLogger(Task.class.getName());

  private final String taskId;
  private final int taskIndex;
  private final List<StageEdge> taskIncomingEdges;
  private final List<StageEdge> taskOutgoingEdges;
  private final ExecutionPropertyMap<VertexExecutionProperty> executionProperties;
  private final DAG<IRVertex, RuntimeEdge<IRVertex>> irDag;
  private final Map<String, Readable> irVertexIdToReadable;
  private final Map<RuntimeEdge, List<String>> downstreamTasks;
  private final Map<RuntimeEdge, List<String>> upstreamTasks;

  public final boolean isStreamVertex;

  private final String pairTaskId;
  private final String pairEdgeId;

  public enum TaskType {
    CRTask,
    MergerTask,
    TransientTask,
    VMTask,
    DefaultTask,
  }

  private final TaskType taskType;
  private final boolean isPartial;
  private final boolean isStateful;
  private final Set<String> o2oEdgeIds;

  /**
   *
   * @param planId               the id of the physical plan.
   * @param taskId               the ID of this task attempt.
   * Constructor.
   * @param executionProperties  {@link VertexExecutionProperty} map for the corresponding stage
   * @param taskIncomingEdges    the incoming edges of the task.
   * @param taskOutgoingEdges    the outgoing edges of the task.
   * @param irVertexIdToReadable the map between IRVertex id to readable.
   */
  public Task(final String taskId,
              final ExecutionPropertyMap<VertexExecutionProperty> executionProperties,
              final DAG<IRVertex, RuntimeEdge<IRVertex>> irDag,
              final List<StageEdge> taskIncomingEdges,
              final List<StageEdge> taskOutgoingEdges,
              final Map<String, Readable> irVertexIdToReadable,
              final String pairTaskId,
              final String pairEdgeId,
              final TaskType taskType,
              final Set<String> o2oEdgeIds) {
    this.taskId = taskId;
    this.taskIndex = RuntimeIdManager.getIndexFromTaskId(taskId);
    this.executionProperties = executionProperties;
    this.irDag = irDag;
    this.taskIncomingEdges = taskIncomingEdges;
    this.taskOutgoingEdges = taskOutgoingEdges;
    this.irVertexIdToReadable = irVertexIdToReadable;
    this.downstreamTasks = calculateDownstreamTasks();
    this.upstreamTasks = calculateUpstreamTasks();
    this.isStreamVertex = irDag.getVertices().size() == 1 && irDag.getVertices().get(0) instanceof StreamVertex;
    LOG.info("Task {} scheduled ... upstreamTasks {}",
      taskId, upstreamTasks.values());

    // find pair task
    this.pairTaskId = pairTaskId;
    this.pairEdgeId = pairEdgeId;
    this.isStateful = irDag.getVertices().stream().anyMatch(vertex -> vertex.isGBK || vertex.isPushback);
    this.o2oEdgeIds = o2oEdgeIds;
    this.isPartial = taskOutgoingEdges.stream().anyMatch(edge -> {
      return edge.getDst().getIRDAG().getVertices().stream()
        .anyMatch(vertex -> vertex instanceof StateMergerVertex);
    });

    /*
    if (pairTaskId != null) {
      this.vmTaskTransientTaskEdge = Pair.of(
         taskOutgoingEdges.stream().filter(edge ->
        !edge.getPropertyValue(AdditionalOutputTagProperty.class).isPresent()).findFirst().get().getId(),
        taskOutgoingEdges.stream().filter(edge ->
        edge.getPropertyValue(AdditionalOutputTagProperty.class).isPresent()).findFirst().get().getId());
    } else {
      this.vmTaskTransientTaskEdge = null;
    }
    */
    this.taskType = taskType;
  }

  /*
  public Pair<String, String> getVmTaskTransientTaskId() {
    return vmTaskTransientTaskId;
  }

  public Pair<String, String> getVmTaskTransientTaskEdge() {
    return vmTaskTransientTaskEdge;
  }
  */

  public boolean isParitalCombine() {
    return isPartial;
  }

  public boolean isStateful() {
    return isStateful;
  }

  public String getPairTaskId() {
    return pairTaskId;
  }

  public String getPairEdgeId() {
    return pairEdgeId;
  }

  public TaskType getTaskType() {
    return taskType;
  }

  public Set<String> getO2oEdgeIds() {
    return o2oEdgeIds;
  }

  public static Task decode(DataInputStream dis,
                            TaskCaching taskCaching) {
    try {
      final String taskId = dis.readUTF();
      final DAG<IRVertex, RuntimeEdge<IRVertex>> irDag = DAG.decode(dis);

      throw new RuntimeException("Not supported");

      /*
      return new Task(taskId,
        null,
        irDag,
        taskCaching.taskIncomingEdges,
        taskCaching.taskOutgoingEdges,
        new HashMap<>());
        */

    } catch (final Exception e) {
      e.printStackTrace();
      throw new RuntimeException(e);
    }
  }

  public static Task decode(DataInputStream dis) {
    try {
      final String taskId = dis.readUTF();
      final DAG<IRVertex, RuntimeEdge<IRVertex>> irDag = DAG.decode(dis);

      int s = dis.readInt();
      final List<StageEdge> taskIncomingEdges = new ArrayList<>(s);
      for (int i = 0; i < s; i++) {
        taskIncomingEdges.add(SerializationUtils.deserialize(dis));
      }
      s = dis.readInt();
      final List<StageEdge> taskOutgoingEdges = new ArrayList<>(s);
      for (int i = 0; i < s; i++) {
        taskOutgoingEdges.add(SerializationUtils.deserialize(dis));
      }
      // final byte[] serializedIRDag = new byte[dis.readInt()];
      // dis.read(serializedIRDag);
      s = dis.readInt();
      final Map<String, Readable> irVertexIdToReadable = new HashMap<>(s);
      for (int i = 0; i < s; i++) {
        final String key = dis.readUTF();
        final Readable val = SerializationUtils.deserialize(dis);
        irVertexIdToReadable.put(key, val);
      }

      final boolean hasPairStageId = dis.readBoolean();
      final String pairStageId;
      if (hasPairStageId) {
        pairStageId = dis.readUTF();
      } else {
        pairStageId = null;
      }

      final boolean hasPairEdgeId = dis.readBoolean();
      final String pairEdgeId;
      if (hasPairEdgeId) {
        pairEdgeId = dis.readUTF();
      } else {
        pairEdgeId = null;
      }

      final TaskType taskType = TaskType.values()[dis.readByte()];
      s = dis.readInt();
      final Set<String> o2oEdges = new HashSet<>(s);
      for (int i = 0; i < s; i++) {
        o2oEdges.add(dis.readUTF());
      }

      return new Task(taskId,
        null,
        irDag,
        taskIncomingEdges,
        taskOutgoingEdges,
        irVertexIdToReadable,
        pairStageId,
        pairEdgeId,
        taskType,
        o2oEdges);
    } catch (final Exception e) {
      e.printStackTrace();
      throw new RuntimeException(e);
    }
  }

  public void encode(final DataOutputStream dos) {
    try {
      dos.writeUTF(taskId);

      irDag.encode(dos);

      dos.writeInt(taskIncomingEdges.size());
      taskIncomingEdges.forEach(edge -> {
        SerializationUtils.serialize(edge, dos);
      });
      dos.writeInt(taskOutgoingEdges.size());
      taskOutgoingEdges.forEach(edge -> {
        SerializationUtils.serialize(edge, dos);
      });
      // dos.writeInt(serializedIRDag.length);
      // dos.write(serializedIRDag);
      dos.writeInt(irVertexIdToReadable.size());
      for (final Map.Entry<String, Readable> entry : irVertexIdToReadable.entrySet()) {
        dos.writeUTF(entry.getKey());
        SerializationUtils.serialize(entry.getValue(), dos);
      }

      if (pairTaskId != null) {
        dos.writeBoolean(true);
        dos.writeUTF(pairTaskId);
      } else {
        dos.writeBoolean(false);
      }

      if (pairEdgeId != null) {
        dos.writeBoolean(true);
        dos.writeUTF(pairEdgeId);
      } else {
        dos.writeBoolean(false);
      }

      dos.writeByte(taskType.ordinal());

      dos.writeInt(o2oEdgeIds.size());
      for (final String edgeId : o2oEdgeIds) {
        dos.writeUTF(edgeId);
      }

    } catch (final Exception e) {
      e.printStackTrace();
      throw new RuntimeException(e);
    }
  }

  public boolean isCrTask() {
    return taskType.equals(TaskType.CRTask);
  }

  public boolean isTransientTask() {
    return taskType.equals(TaskType.TransientTask);
  }

  public boolean isVMTask() {
    return taskType.equals(TaskType.VMTask);
  }

  public boolean isMerger() { return taskType.equals(TaskType.MergerTask); }

  public boolean isDefaultTask() {
    return taskType.equals(TaskType.DefaultTask);
  }

  public boolean isSourceTask() {
    return !irVertexIdToReadable.isEmpty();
  }

  /**
   * @return the serialized IR DAG of the task.
   */
  public DAG<IRVertex, RuntimeEdge<IRVertex>> getIrDag() {
    return irDag;
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

  public Map<RuntimeEdge, List<String>> getDownstreamTasks() {
    return downstreamTasks;
  }

  public Map<RuntimeEdge, List<String>> getUpstreamTasks() {
    return upstreamTasks;
  }

  private Map<RuntimeEdge, List<String>> calculateDownstreamTasks() {
    return taskOutgoingEdges.stream().map(edge -> {
      final int parallelism = edge
        .getSrcIRVertex().getPropertyValue(ParallelismProperty.class).get();

      final CommunicationPatternProperty.Value comm =
        edge.getPropertyValue(CommunicationPatternProperty.class).get();

      if (comm.equals(CommunicationPatternProperty.Value.OneToOne)
        || comm.equals(CommunicationPatternProperty.Value.TransientOneToOne)) {
        return Pair.of(edge, Collections.singletonList(
          RuntimeIdManager.generateTaskId(edge.getDst().getId(), taskIndex, 0)));
      } else {
        return Pair.of(edge, getDstTaskIds(taskId, edge));
      }
    }).collect(Collectors.toMap(Pair::left, Pair::right));
  }

  private List<String> getDstTaskIds(final String taskId,
                                     final RuntimeEdge runtimeEdge) {
    final Optional<CommunicationPatternProperty.Value> comValue =
      runtimeEdge.getPropertyValue(CommunicationPatternProperty.class);

    final StageEdge stageEdge = (StageEdge) runtimeEdge;
    final int index = RuntimeIdManager.getIndexFromTaskId(taskId);

    final List<String> dstTaskIds;
    if (comValue.get().equals(CommunicationPatternProperty.Value.OneToOne)
      || comValue.get().equals(CommunicationPatternProperty.Value.TransientOneToOne)) {
      dstTaskIds = Collections.singletonList(
        RuntimeIdManager.generateTaskId(stageEdge.getDst().getId(), index, 0));
    } else if (comValue.get().equals(CommunicationPatternProperty.Value.BroadCast)
      || comValue.get().equals(CommunicationPatternProperty.Value.Shuffle)
      || comValue.get().equals(CommunicationPatternProperty.Value.TransientShuffle)
      || comValue.get().equals(CommunicationPatternProperty.Value.TransientRR)
      || comValue.get().equals(CommunicationPatternProperty.Value.RoundRobin) ) {

      final List<Integer> dstIndices = stageEdge.getDst().getTaskIndices();
      dstTaskIds =
        dstIndices.stream()
          .map(dstTaskIndex ->
            RuntimeIdManager.generateTaskId(stageEdge.getDst().getId(), dstTaskIndex, 0))
          .collect(Collectors.toList());
      LOG.info("Writing data: edge: {}, Task {}, Dest {}", runtimeEdge.getId(), taskId, dstIndices);
    } else {
      throw new UnsupportedCommPatternException(new Exception("Communication pattern not supported"));
    }
    return dstTaskIds;
  }

  private Map<RuntimeEdge, List<String>> calculateUpstreamTasks() {
    return taskIncomingEdges.stream().map(edge -> {
      final int parallelism = edge
        .getSrcIRVertex().getPropertyValue(ParallelismProperty.class).get();

      final CommunicationPatternProperty.Value comm =
        edge.getPropertyValue(CommunicationPatternProperty.class).get();

      if (comm.equals(CommunicationPatternProperty.Value.OneToOne)
        || comm.equals(CommunicationPatternProperty.Value.TransientOneToOne)) {
        return Pair.of(edge, Collections.singletonList(
          RuntimeIdManager.generateTaskId(edge.getSrc().getId(), taskIndex, 0)));
      } else {
        return Pair.of(edge, getSrcTaskIds(taskId, edge));
      }
    }).collect(Collectors.toMap(Pair::left, Pair::right));
  }


  private List<String> getSrcTaskIds(final String taskId,
                                           final RuntimeEdge runtimeEdge) {
    final Optional<CommunicationPatternProperty.Value> comValue =
      runtimeEdge.getPropertyValue(CommunicationPatternProperty.class);

    final StageEdge stageEdge = (StageEdge) runtimeEdge;
    final int index = RuntimeIdManager.getIndexFromTaskId(taskId);

    final List<String> dstTaskIds;
    if (comValue.get().equals(CommunicationPatternProperty.Value.OneToOne)
      || comValue.get().equals(CommunicationPatternProperty.Value.TransientOneToOne)) {
      dstTaskIds = Collections.singletonList(
        RuntimeIdManager.generateTaskId(stageEdge.getSrc().getId(), index, 0));
    } else if (comValue.get().equals(CommunicationPatternProperty.Value.BroadCast)
      || comValue.get().equals(CommunicationPatternProperty.Value.Shuffle)
      || comValue.get().equals(CommunicationPatternProperty.Value.TransientShuffle)
      || comValue.get().equals(CommunicationPatternProperty.Value.TransientRR)
      || comValue.get().equals(CommunicationPatternProperty.Value.RoundRobin) ) {

      final List<Integer> dstIndices = stageEdge.getSrc().getTaskIndices();
      dstTaskIds =
        dstIndices.stream()
          .map(dstTaskIndex ->
            RuntimeIdManager.generateTaskId(stageEdge.getSrc().getId(), dstTaskIndex, 0))
          .collect(Collectors.toList());
      LOG.info("Writing data: edge: {}, Task {}, Dest {}", runtimeEdge.getId(), taskId, dstIndices);
    } else {
      throw new UnsupportedCommPatternException(new Exception("Communication pattern not supported"));
    }
    return dstTaskIds;
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

  public void setProperty(final VertexExecutionProperty<?> executionProperty) {
    executionProperties.put(executionProperty);
  }

  /**
   * @return the map between IRVertex id and readable.
   */
  public Map<String, Readable> getIrVertexIdToReadable() {
    return irVertexIdToReadable;
  }

  @Override
  public String toString() {
    return taskId;
    /*
    final StringBuilder sb = new StringBuilder();
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
    */
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    Task task = (Task) o;
    return Objects.equals(taskId, task.taskId);
  }

  @Override
  public int hashCode() {

    return Objects.hash(taskId);
  }
}
