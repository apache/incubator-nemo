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

import java.util.ArrayList;
import java.util.List;

/**
 * Metric class for {@link edu.snu.nemo.runtime.common.plan.Task}.
 */
public class TaskMetric implements Metric<TaskState.State> {
  private String id;
  private List<StateTransitionEvent<TaskState.State>> stateTransitionEvents = new ArrayList<>();
  private int readBytes = 0;
  private int writtenBytes = 0;

  public TaskMetric(final String id) {
    this.id = id;
  }

  public final int getReadBytes() {
    return readBytes;
  }

  public final void setReadBytes(final int readBytes) {
    this.readBytes = readBytes;
  }

  public final int getWrittenBytes() {
    return writtenBytes;
  }

  public final void setWrittenBytes(final int writtenBytes) {
    this.writtenBytes = writtenBytes;
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

  @Override
  public void processMetricMessage(final String metricField, final byte[] metricValue) {
    // do nothing
  }
}
