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

import java.util.List;

/**
 * Interface for all metrics.
 * @param <T> class of state of the metric.
 */
public interface Metric<T> {
  /**
   * Get its unique id.
   * @return an unique id
   */
  String getId();

  /**
   * Get its list of {@link Event}.
   * @return list of events.
   */
  List<Event<T>> getEvents();

  /**
   * Add a {@link Event} to the metric.
   * @param prevState previous state.
   * @param newState new state.
   */
  void addEvent(final T prevState, final T newState);
}
