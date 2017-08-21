/*
 * Copyright (C) 2017 Seoul National University
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
package edu.snu.vortex.runtime.executor.metric;

import edu.snu.vortex.runtime.exception.JsonParseException;
import org.apache.reef.tang.annotations.DefaultImplementation;

import java.util.Map;

/**
 * Interface for metric sender.
 */
@DefaultImplementation(PeriodicMetricSender.class)
public interface MetricSender extends AutoCloseable {

  /**
   * Send the json metric data to driver.
   * @param jsonMetricData json metric data
   * @throws JsonParseException throws when the json data format is not correct
   */
  void send(Map<String, Object> jsonMetricData) throws JsonParseException;
}
