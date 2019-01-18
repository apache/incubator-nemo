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

import org.apache.nemo.runtime.common.state.TaskState;
import org.apache.commons.lang3.SerializationUtils;

import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Metric class for {@link org.apache.nemo.runtime.common.plan.Task}.
 */
public class TaskMetric implements StateMetric<TaskState.State> {
  private String id;
  private List<StateTransitionEvent<TaskState.State>> stateTransitionEvents = new ArrayList<>();
  private long serializedReadBytes = -1;
  private long encodedReadBytes = -1;
  private long writtenBytes = -1;
  private long boundedSourceReadTime = -1;
  private long taskDeserializationTime = -1;
  private int scheduleAttempt = -1;
  private String containerId = "";

  private static final Logger LOG = LoggerFactory.getLogger(TaskMetric.class.getName());

  public TaskMetric(final String id) {
    this.id = id;
  }

  public final long getSerializedReadBytes() {
    return serializedReadBytes;
  }

  private void setSerializedReadBytes(final long serializedReadBytes) {
    this.serializedReadBytes = serializedReadBytes;
  }

  public final long getEncodedReadBytes() {
    return encodedReadBytes;
  }

  private void setEncodedReadBytes(final long encodedReadBytes) {
    this.encodedReadBytes = encodedReadBytes;
  }

  public final long getBoundedSourceReadTime() {
    return boundedSourceReadTime;
  }

  private void setBoundedSourceReadTime(final long boundedSourceReadTime) {
    this.boundedSourceReadTime = boundedSourceReadTime;
  }

  public final long getTaskDeserializationTime() {
    return taskDeserializationTime;
  }

  private void setTaskDeserializationTime(final long taskDeserializationTime) {
    this.taskDeserializationTime = taskDeserializationTime;
  }

  public final long getWrittenBytes() {
    return writtenBytes;
  }

  private void setWrittenBytes(final long writtenBytes) {
    this.writtenBytes = writtenBytes;
  }

  public final int getScheduleAttempt() {
    return scheduleAttempt;
  }

  private void setScheduleAttempt(final int scheduleAttempt) {
    this.scheduleAttempt = scheduleAttempt;
  }

  public final String getContainerId() {
    return containerId;
  }

  private void setContainerId(final String containerId) {
    this.containerId = containerId;
  }

  @Override
  public final List<StateTransitionEvent<TaskState.State>> getStateTransitionEvents() {
    return stateTransitionEvents;
  }

  @Override
  public final String getId() {
    return id;
  }

  @Override
  public final void addEvent(final TaskState.State prevState, final TaskState.State newState) {
    stateTransitionEvents.add(new StateTransitionEvent<>(System.currentTimeMillis(), prevState, newState));
  }

  private void addEvent(final StateTransitionEvent<TaskState.State> event) {
    stateTransitionEvents.add(event);
  }

  @Override
  public final boolean processMetricMessage(final String metricField, final byte[] metricValue) {
    LOG.debug("metric {} is just arrived!", metricField);
    switch (metricField) {
      case "serializedReadBytes":
        setSerializedReadBytes(SerializationUtils.deserialize(metricValue));
        break;
      case "encodedReadBytes":
        setEncodedReadBytes(SerializationUtils.deserialize(metricValue));
        break;
      case "boundedSourceReadTime":
        setBoundedSourceReadTime(SerializationUtils.deserialize(metricValue));
        break;
      case "writtenBytes":
        setWrittenBytes(SerializationUtils.deserialize(metricValue));
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
      default:
        LOG.warn("metricField {} is not supported.", metricField);
        return false;
    }
    return true;
  }
}
