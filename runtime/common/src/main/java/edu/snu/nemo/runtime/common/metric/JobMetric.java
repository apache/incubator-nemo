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

import edu.snu.nemo.runtime.common.plan.PhysicalPlan;
import edu.snu.nemo.runtime.common.state.JobState;

import java.util.ArrayList;
import java.util.List;

/**
 * Metric class for Job (or {@link PhysicalPlan}).
 */
public final class JobMetric implements Metric<JobState.State> {
  private String id;
  private List<Event<JobState.State>> events = new ArrayList<>();

  public JobMetric(final PhysicalPlan physicalPlan) {
    this.id = physicalPlan.getId();
  }

  public JobMetric(final String id) {
    this.id = id;
  }

  @Override
  public String getId() {
    return id;
  }

  @Override
  public List<Event<JobState.State>> getEvents() {
    return events;
  }

  @Override
  public void addEvent(final JobState.State prevState, final JobState.State newState) {
    events.add(new Event<>(System.currentTimeMillis(), prevState, newState));
  }

  @Override
  public void processMetricMessage(final String metricField, final byte[] metricValue) {
    // do nothing
  }
}
