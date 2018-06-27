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
package edu.snu.nemo.runtime.common.metric;

import edu.snu.nemo.runtime.common.state.TaskState;
import org.apache.commons.lang3.SerializationUtils;

import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Metric class for {@link edu.snu.nemo.runtime.common.plan.Task}.
 */
public class TaskMetric implements Metric<TaskState.State> {
  private String id;
  private List<StateTransitionEvent<TaskState.State>> stateTransitionEvents = new ArrayList<>();
  private List<DataTransferEvent> dataTransferEvents = new ArrayList<>();
  private long readBytes = -1;
  private long writtenBytes = -1;
  private int scheduleAttempt = -1;
  private String containerId = "";

  private static final Logger LOG = LoggerFactory.getLogger(TaskMetric.class.getName());

  public TaskMetric(final String id) {
    this.id = id;
  }

  public final long getReadBytes() {
    return readBytes;
  }

  private void setReadBytes(final long readBytes) {
    this.readBytes = readBytes;
  }

  public final long getWrittenBytes() {
    return writtenBytes;
  }

  private void setWrittenBytes(final long writtenBytes) {
    this.writtenBytes = writtenBytes;
  }

  public final List<DataTransferEvent> getDataTransferEvents() {
    return dataTransferEvents;
  }

  private void addDataTransferEvent(final DataTransferEvent event) {
    dataTransferEvents.add(event);
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
  public final void processMetricMessage(final String metricField, final byte[] metricValue) {
    LOG.info("metric {} is just arrived!", metricField);
    switch (metricField) {
      case "readBytes":
        setReadBytes(SerializationUtils.deserialize(metricValue));
        break;
      case "writtenBytes":
        setWrittenBytes(SerializationUtils.deserialize(metricValue));
        break;
      case "event":
        final StateTransitionEvent<TaskState.State> newStateTransitionEvent =
            SerializationUtils.deserialize(metricValue);
        addEvent(newStateTransitionEvent);
        break;
      case "dataEvent":
        final DataTransferEvent newDataTransferEvent = SerializationUtils.deserialize(metricValue);
        addDataTransferEvent(newDataTransferEvent);
        break;
      case "scheduleAttempt":
        setScheduleAttempt(SerializationUtils.deserialize(metricValue));
        break;
      case "containerId":
        setContainerId(SerializationUtils.deserialize(metricValue));
        break;
      default:
        LOG.warn("metricField {} is not supported.", metricField);
    }
  }
}
