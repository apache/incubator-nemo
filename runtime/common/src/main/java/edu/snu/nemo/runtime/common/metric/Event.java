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

import java.io.Serializable;

/**
 * Class for all generic event that contains timestamp at the moment.
 */
public class Event implements Serializable {
  private long timestamp;

  /**
   * Constructor.
   * @param timestamp timestamp in millisecond.
   */
  public Event(final long timestamp) {
    this.timestamp = timestamp;
  }

  /**
   * Get timestamp.
   * @return timestamp.
   */
  public final long getTimestamp() {
    return timestamp;
  };

  /**
   * Set timestamp.
   * @param timestamp timestamp in millisecond.
   */
  public final void setTimestamp(final long timestamp) {
    this.timestamp = timestamp;
  }
}
