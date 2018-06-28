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

/**
 * Event for metric. It contains timestamp and the state transition.
 * @param <T> class of state for the metric.
 */
public final class Event<T> {
  private Long timestamp;
  private T prevState;
  private T newState;

  public Event(final Long timestamp, final T prevState, final T newState) {
    this.timestamp = timestamp;
    this.prevState = prevState;
    this.newState = newState;
  }

  /**
   * Get timestamp.
   * @return timestamp.
   */
  public Long getTimestamp() {
    return timestamp;
  }

  /**
   * Get previous state.
   * @return previous state.
   */
  public T getPrevState() {
    return prevState;
  }

  /**
   * Get new state.
   * @return new state.
   */
  public T getNewState() {
    return newState;
  }
}
