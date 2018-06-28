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
package edu.snu.nemo.runtime.master.metric;

import edu.snu.nemo.runtime.common.state.TaskState;

import java.util.ArrayList;
import java.util.List;

/**
 * Metric class for {@link edu.snu.nemo.runtime.common.plan.Task}.
 */
public class TaskMetric implements Metric<TaskState.State> {
  private String id;
  private List<Event<TaskState.State>> events = new ArrayList<>();

  public TaskMetric(final String id) {
    this.id = id;
  }

  @Override
  public final List<Event<TaskState.State>> getEvents() {
    return events;
  }

  @Override
  public final String getId() {
    return id;
  }

  @Override
  public final void addEvent(final TaskState.State prevState, final TaskState.State newState) {
    events.add(new Event<>(System.currentTimeMillis(), prevState, newState));
  }
}
