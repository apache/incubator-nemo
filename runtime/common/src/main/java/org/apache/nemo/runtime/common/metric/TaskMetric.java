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
package org.apache.nemo.runtime.common.metric;

import org.apache.commons.lang3.SerializationUtils;
import org.apache.nemo.runtime.common.state.TaskState;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

/**
 * Metric class for {@link org.apache.nemo.runtime.common.plan.Task}.
 */
public class TaskMetric implements StateMetric<TaskState.State> {
  private String id;
  private String containerId = "";
  private int scheduleAttempt = -1;
  private List<StateTransitionEvent<TaskState.State>> stateTransitionEvents = new ArrayList<>();
  private long taskDuration = -1;
  private long numOfProcessedElements = -1;
  private long taskCPUTime = -1;
  private long schedulingOverhead = -1;
  private long serializedReadBytes = -1;
  private long encodedReadBytes = -1;
  private long taskOutputBytes = -1;
  private long taskSerializationTime = -1;
  private long taskDeserializationTime = -1;
  private long boundedSourceReadTime = -1;
  private long peakExecutionMemory = -1;
  private int taskSizeRatio = -1;
  private long shuffleReadBytes = -1;
  private long shuffleReadTime = -1;
  private long shuffleWriteBytes = -1;
  private long shuffleWriteTime = -1;

  private static final Logger LOG = LoggerFactory.getLogger(TaskMetric.class.getName());

  public TaskMetric(final String id) {
    this.id = id;
  }

  /**
   * Method related to container Id.
   */
  public final String getContainerId() {
    return this.containerId;
  }

  private void setContainerId(final String containerId) {
    this.containerId = containerId;
  }

  /**
   * Method related to schedule attempt.
   */
  public final int getScheduleAttempt() {
    return scheduleAttempt;
  }

  private void setScheduleAttempt(final int scheduleAttempt) {
    this.scheduleAttempt = scheduleAttempt;
  }

  /**
   * Method related to state transition events.
   */
  @Override
  public final List<StateTransitionEvent<TaskState.State>> getStateTransitionEvents() {
    return stateTransitionEvents;
  }

  @Override
  public final void addEvent(final TaskState.State prevState, final TaskState.State newState) {
    stateTransitionEvents.add(new StateTransitionEvent<>(System.currentTimeMillis(), prevState, newState));
  }

  private void addEvent(final StateTransitionEvent<TaskState.State> event) {
    stateTransitionEvents.add(event);
  }

  /**
   * Method related to task duration.
   */
  public final long getTaskDuration() {
    return this.taskDuration;
  }

  private void setTaskDuration(final long taskDuration) {
    this.taskDuration = taskDuration;
  }

  /**
   * Getter of numOfProcessedElements.
   * @return numOfProcessedElements.
   */
  public final long getNumOfProcessedElements() {
    return numOfProcessedElements;
  }

  /**
   * Setter of numOfProcessedElements.
   * @param numOfProcessedElements to set.
   */
  public final void setNumOfProcessedElements(final long numOfProcessedElements) {
    this.numOfProcessedElements = numOfProcessedElements;
  }

  /**
   * Method related to task CPU time.
   */
  public final long getTaskCPUTime() {
    return this.taskCPUTime;
  }

  private void setTaskCPUTime(final long taskCPUTime) {
    this.taskCPUTime = taskCPUTime;
  }

  /**
   * Method related to scheduling overhead.
   */
  public final long getSchedulingOverhead() {
    return this.schedulingOverhead;
  }

  private void setSchedulingOverhead(final long schedulingOverhead) {
    this.schedulingOverhead = schedulingOverhead;
  }

  /**
   * Method related to serialized read bytes.
   * serialized = encoded + compressed
   */
  public final long getSerializedReadBytes() {
    return serializedReadBytes;
  }

  private void setSerializedReadBytes(final long serializedReadBytes) {
    this.serializedReadBytes = serializedReadBytes;
  }

  /**
   * Method related to encoded read bytes.
   */
  public final long getEncodedReadBytes() {
    return encodedReadBytes;
  }

  private void setEncodedReadBytes(final long encodedReadBytes) {
    this.encodedReadBytes = encodedReadBytes;
  }

  /**
   * Method related to task output bytes.
   */
  public final long getTaskOutputBytes() {
    return taskOutputBytes;
  }

  private void setTaskOutputBytes(final long taskOutputBytes) {
    this.taskOutputBytes = taskOutputBytes;
  }

  /**
   * Method related to task serialization time.
   */
  public final long getTaskSerializationTime() {
    return taskSerializationTime;
  }

  private void setTaskSerializationTime(final long taskSerializationTime) {
    this.taskSerializationTime = taskSerializationTime;
  }

  /**
   * Method related to task deserialization time.
   */
  public final long getTaskDeserializationTime() {
    return taskDeserializationTime;
  }

  private void setTaskDeserializationTime(final long taskDeserializationTime) {
    this.taskDeserializationTime = taskDeserializationTime;
  }

  /**
   * Method related to bounded source read time.
   */
  public final long getBoundedSourceReadTime() {
    return boundedSourceReadTime;
  }

  private void setBoundedSourceReadTime(final long boundedSourceReadTime) {
    this.boundedSourceReadTime = boundedSourceReadTime;
  }

  /**
   * Method related to peak execution memory.
   */
  public final long getPeakExecutionMemory() {
    return this.peakExecutionMemory;
  }

  private void setPeakExecutionMemory(final long peakExecutionMemory) {
    this.peakExecutionMemory = peakExecutionMemory;
  }

  /**
   * Method related to task size ratio.
   */
  public final int getTaskSizeRatio() {
    return this.taskSizeRatio;
  }

  private void setTaskSizeRatio(final int taskSizeRatio) {
    this.taskSizeRatio = taskSizeRatio;
  }

  /**
   * Method related to shuffle.
   */
  public final long getShuffleReadBytes() {
    return this.shuffleReadBytes;
  }

  private void setShuffleReadBytes(final long shuffleReadBytes) {
    this.shuffleReadBytes = shuffleReadBytes;
  }

  public final long getShuffleReadTime() {
    return this.shuffleReadTime;
  }

  private void setShuffleReadTime(final long shuffleReadTime) {
    this.shuffleReadTime = shuffleReadTime;
  }

  public final long getShuffleWriteBytes() {
    return this.shuffleWriteBytes;
  }

  private void setShuffleWriteBytes(final long shuffleWriteBytes) {
    this.shuffleWriteBytes = shuffleWriteBytes;
  }

  public final long getShuffleWriteTime() {
    return this.shuffleWriteTime;
  }

  private void setShuffleWriteTime(final long shuffleWriteTime) {
    this.shuffleWriteTime = shuffleWriteTime;
  }

  @Override
  public final String getId() {
    return id;
  }

  @Override
  public final boolean processMetricMessage(final String metricField, final byte[] metricValue) {
    LOG.debug("metric {} has just arrived!", metricField);
    switch (metricField) {
      case "taskDuration":
        setTaskDuration(SerializationUtils.deserialize(metricValue));
        break;
      case "numOfProcessedElements":
        setNumOfProcessedElements(SerializationUtils.deserialize(metricValue));
        break;
      case "schedulingOverhead":
        setSchedulingOverhead(SerializationUtils.deserialize(metricValue));
        break;
      case "serializedReadBytes":
        setSerializedReadBytes(SerializationUtils.deserialize(metricValue));
        break;
      case "encodedReadBytes":
        setEncodedReadBytes(SerializationUtils.deserialize(metricValue));
        break;
      case "boundedSourceReadTime":
        setBoundedSourceReadTime(SerializationUtils.deserialize(metricValue));
        break;
      case "taskOutputBytes":
        setTaskOutputBytes(SerializationUtils.deserialize(metricValue));
        break;
      case "taskDeserializationTime":
        setTaskDeserializationTime(SerializationUtils.deserialize(metricValue));
        break;
      case "stateTransitionEvent":
        final StateTransitionEvent<TaskState.State> newStateTransitionEvent =
          SerializationUtils.deserialize(metricValue);
        addEvent(newStateTransitionEvent);
        break;
      case "scheduleAttempt":
        setScheduleAttempt(SerializationUtils.deserialize(metricValue));
        break;
      case "containerId":
        setContainerId(SerializationUtils.deserialize(metricValue));
        break;
      case "taskCPUTime":
        setTaskCPUTime(SerializationUtils.deserialize(metricValue));
        break;
      case "taskSerializationTime":
        setTaskSerializationTime(SerializationUtils.deserialize(metricValue));
        break;
      case "peakExecutionMemory":
        setPeakExecutionMemory(SerializationUtils.deserialize(metricValue));
        break;
      case "taskSizeRatio":
        setTaskSizeRatio(SerializationUtils.deserialize(metricValue));
        break;
      case "shuffleReadBytes":
        setShuffleReadBytes(SerializationUtils.deserialize(metricValue));
        break;
      case "shuffleReadTime":
        setShuffleReadTime(SerializationUtils.deserialize(metricValue));
        break;
      case "shuffleWriteBytes":
        setShuffleWriteBytes(SerializationUtils.deserialize(metricValue));
        break;
      case "shuffleWriteTime":
        setShuffleWriteTime(SerializationUtils.deserialize(metricValue));
        break;
      default:
        LOG.warn("metricField {} is not supported.", metricField);
        return false;
    }
    return true;
  }
}
