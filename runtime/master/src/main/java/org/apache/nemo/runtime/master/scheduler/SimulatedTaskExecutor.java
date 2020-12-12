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

package org.apache.nemo.runtime.master.scheduler;

import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.collect.Streams;
import org.apache.commons.lang3.SerializationUtils;
import org.apache.nemo.common.dag.DAG;
import org.apache.nemo.common.ir.vertex.IRVertex;
import org.apache.nemo.common.ir.vertex.executionproperty.ParallelismProperty;
import org.apache.nemo.runtime.common.RuntimeIdManager;
import org.apache.nemo.runtime.common.comm.ControlMessage;
import org.apache.nemo.runtime.common.message.MessageEnvironment;
import org.apache.nemo.runtime.common.message.MessageUtils;
import org.apache.nemo.runtime.common.metric.JobMetric;
import org.apache.nemo.runtime.common.metric.StateTransitionEvent;
import org.apache.nemo.runtime.common.metric.TaskMetric;
import org.apache.nemo.runtime.common.plan.RuntimeEdge;
import org.apache.nemo.runtime.common.plan.Task;
import org.apache.nemo.runtime.common.state.TaskState;
import org.apache.nemo.runtime.master.metric.MetricStore;
import org.apache.nemo.runtime.master.resource.ExecutorRepresenter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

/**
 * Class for simulated task execution.
 */
public final class SimulatedTaskExecutor {
  private static final Logger LOG = LoggerFactory.getLogger(SimulatedTaskExecutor.class.getName());
  private static final String TASK_METRIC_ID = "TaskMetric";
  private static final int SAMPLING_PARALLELISM = 32;
  private static final int FALLBACK_PARALLELISM = 1;
  /**
   * the simulation scheduler that the executor is associated with.
   */
  private final SimulationScheduler scheduler;
  private final ExecutorRepresenter executorRepresenter;
  private Long executorInitializationTime;
  private final AtomicLong currentTime;
  private Long timeCheckpoint;
  private final MetricStore actualMetricStore;
  private final ConcurrentMap<String, DAG<IRVertex, RuntimeEdge<IRVertex>>> stageIDToStageIRDAG;

  /**
   * Constructor.
   * @param scheduler the simulation scheduler that the executor is associated with.
   */
  SimulatedTaskExecutor(final SimulationScheduler scheduler,
                        final ExecutorRepresenter executorRepresenter,
                        final MetricStore actualMetricStore) {
    this.scheduler = scheduler;
    this.executorRepresenter = executorRepresenter;
    this.actualMetricStore = actualMetricStore;
    this.stageIDToStageIRDAG = new ConcurrentHashMap<>();
    this.currentTime = new AtomicLong(-1L);
    this.executorInitializationTime = -1L;
  }

  /**
   * Calculate the expected task duration.
   * This only works if there exists metrics in the actual MetricStore, that contains information about the stage,
   * as well as with the parallelism the task is associated with.
   *
   * @param task the task to calculate the task duration for.
   * @return the expected task duration.
   */
  private long calculateExpectedTaskDuration(final Task task) {
    final DAG<IRVertex, RuntimeEdge<IRVertex>> stageIRDAG = stageIDToStageIRDAG.computeIfAbsent(task.getStageId(),
      i -> SerializationUtils.deserialize(task.getSerializedIRDag()));

    final Map<String, Object> jobMetricMap = this.actualMetricStore.getMetricMap(JobMetric.class);
    if (jobMetricMap.size() > 1) {
      LOG.warn("MetricStore has more than one JobMetric. The results could be misleading.");
    }
    // Fetch first element.
    final JobMetric jobMetric = (JobMetric) jobMetricMap.entrySet().iterator().next().getValue();
    final JsonNode stageDAG = jobMetric.getStageDAG();

    // Gather ID of stages that have the characteristics that the task possesses.
    final Set<String> stageIdsToGatherMetricsFrom = Streams.stream(() -> stageDAG.get("vertices").iterator())
      .filter(s -> s.get("properties").get("irDag").get("vertices").size()
        == stageIRDAG.getVertices().size())  // same # of vertices.
      .filter(s -> s.get("properties").get("irDag").get("edges").size()
        == stageIRDAG.getEdges().size())  // same # of edges.
      .filter(s -> s.get("properties").get("executionProperties")
        .get("org.apache.nemo.common.ir.vertex.executionproperty.ParallelismProperty").asInt(1)
        == SAMPLING_PARALLELISM)  // sampling vertices have parallelism of 32
      .filter(s -> s.get("properties").get("executionProperties")
        .get("org.apache.nemo.common.ir.vertex.executionproperty.EnableDynamicTaskSizingProperty").asBoolean())
      .map(s -> s.get("id").asText())
      .collect(Collectors.toSet());

    final int simulationTaskParallelism = task.getPropertyValue(ParallelismProperty.class).orElse(FALLBACK_PARALLELISM);
    // Derive the average task duration from the stages.
    final OptionalDouble average = this.actualMetricStore.getMetricMap(TaskMetric.class).entrySet().stream()
      .filter(e -> stageIdsToGatherMetricsFrom.contains(RuntimeIdManager.getStageIdFromTaskId(e.getKey())))
      .map(Map.Entry::getValue)  // stream of TaskMetric.
      .filter(tm -> ((TaskMetric) tm).getTaskSizeRatio() == simulationTaskParallelism)
      .mapToLong(tm -> ((TaskMetric) tm).getTaskDuration())
      .filter(l -> l > 0)
      .average();

    // convert to long and save.
    return (long) (average.orElse(0) + 0.5);  // 0 to indicate something went wrong
  }

  /**
   * Handle the task and record metrics, as a real Executor#onTaskReceived would.
   *
   * @param task the task to execute.
   */
  public void onTaskReceived(final Task task) {
    if (executorInitializationTime < 0) {
      executorInitializationTime = System.currentTimeMillis();
      currentTime.set(executorInitializationTime);
      timeCheckpoint = executorInitializationTime;
    }
    final String taskId = task.getTaskId();
    final int attemptIdx = task.getAttemptIdx();
    String idOfVertexPutOnHold = null;

    final long schedulingOverhead = System.currentTimeMillis() - this.timeCheckpoint;
    this.timeCheckpoint = System.currentTimeMillis();
    this.sendMetric(TASK_METRIC_ID, taskId, "schedulingOverhead",
      SerializationUtils.serialize(schedulingOverhead));
    final long executionStartTime = this.currentTime.getAndAdd(schedulingOverhead);

    // Prepare (constructor of TaskExecutor)
    // Deserialize task
    // Connect incoming / outgoing edges.

    // Execute
    LOG.debug("{} started", taskId);

    // Fetch external data (Read) and process them
    // Finalize vertex and write

    final long expectedTaskDuration = this.calculateExpectedTaskDuration(task);
    this.currentTime.getAndAdd(expectedTaskDuration);

    this.sendMetric(TASK_METRIC_ID, taskId, "taskDuration",
      SerializationUtils.serialize(this.currentTime.get() - executionStartTime));
    this.timeCheckpoint = System.currentTimeMillis();
    if (idOfVertexPutOnHold == null) {
      this.onTaskStateChanged(taskId, attemptIdx, TaskState.State.COMPLETE,
        Optional.empty(), Optional.empty());
      LOG.debug("{} completed", taskId);
    } else {
      this.onTaskStateChanged(taskId, attemptIdx, TaskState.State.ON_HOLD,
        Optional.of(idOfVertexPutOnHold), Optional.empty());
      LOG.debug("{} on hold", taskId);
    }
  }

  /**
   *
   * @return the elapsed time for the executor.
   */
  public Long getElapsedTime() {
    return currentTime.get() - executorInitializationTime;
  }

  /**
   * Updates the state of the task.
   *
   * @param taskId          of the task.
   * @param attemptIdx      of the task.
   * @param newState        of the task.
   * @param vertexPutOnHold the vertex put on hold.
   * @param cause           only provided as non-empty upon recoverable failures.
   */
  private void onTaskStateChanged(final String taskId,
                                  final int attemptIdx,
                                  final TaskState.State newState,
                                  final Optional<String> vertexPutOnHold,
                                  final Optional<TaskState.RecoverableTaskFailureCause> cause) {
    this.sendMetric("TaskMetric", taskId,
      "stateTransitionEvent", SerializationUtils.serialize(new StateTransitionEvent<>(
        this.currentTime.get(), null, newState
      )));

    final ControlMessage.TaskStateChangedMsg.Builder msgBuilder =
      ControlMessage.TaskStateChangedMsg.newBuilder()
        .setExecutorId(executorRepresenter.getExecutorId())
        .setTaskId(taskId)
        .setAttemptIdx(attemptIdx)
        .setState(MessageUtils.convertState(newState));
    if (newState == TaskState.State.ON_HOLD && vertexPutOnHold.isPresent()) {
      msgBuilder.setVertexPutOnHoldId(vertexPutOnHold.get());
    }
    cause.ifPresent(c -> msgBuilder.setFailureCause(MessageUtils.convertFailureCause(c)));

    // Send taskStateChangedMsg to master!
    this.sendControlMessage(
      ControlMessage.Message.newBuilder()
        .setId(RuntimeIdManager.generateMessageId())
        .setListenerId(MessageEnvironment.RUNTIME_MASTER_MESSAGE_LISTENER_ID)
        .setType(ControlMessage.MessageType.TaskStateChanged)
        .setTaskStateChangedMsg(msgBuilder.build())
        .build());
  }

  /**
   * Send the control message to the scheduler, as an executor would. Main handles TaskStateChanged messages.
   *
   * @param message control message to send.
   */
  private void sendControlMessage(final ControlMessage.Message message) {
    this.executorRepresenter.sendControlMessage(message);
  }

  /**
   * Send the metric to the scheduler, as an executor would. See where it is used in MetricMessageSender#send.
   *
   * @param metricType  type of metric.
   * @param metricId    id of metric.
   * @param metricField field of metric.
   * @param metricValue value of metric.
   */
  public void sendMetric(final String metricType, final String metricId,
                         final String metricField, final byte[] metricValue) {
    this.scheduler.handleMetricMessage(metricType, metricId, metricField, metricValue);
  }
}
