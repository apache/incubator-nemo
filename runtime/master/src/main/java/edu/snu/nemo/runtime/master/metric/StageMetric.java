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

import edu.snu.nemo.runtime.common.plan.Stage;
import edu.snu.nemo.runtime.common.state.StageState;

import java.util.ArrayList;
import java.util.List;

/**
 * Metric class for {@link Stage}.
 */
public class StageMetric implements Metric<StageState.State> {
  private String id;
  private List<Event<StageState.State>> events = new ArrayList<>();

  public StageMetric(final Stage stage) {
    this.id = stage.getId();
  }

  public StageMetric(final String id) {
    this.id = id;
  }

  @Override
  public final String getId() {
    return id;
  }

  @Override
  public final List<Event<StageState.State>> getEvents() {
    return events;
  }

  @Override
  public final void addEvent(final StageState.State prevState, final StageState.State newState) {
    events.add(new Event<>(System.currentTimeMillis(), prevState, newState));
  }
}
