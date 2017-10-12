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
package edu.snu.vortex.runtime.master;

import javax.inject.Inject;

import edu.snu.vortex.runtime.common.metric.MetricMessageHandler;
import org.apache.reef.annotations.audience.DriverSide;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

/**
 * A default metric message handler.
 */
@DriverSide
public final class MetricManagerMaster implements MetricMessageHandler {

  private static final Logger LOG = LoggerFactory.getLogger(MetricManagerMaster.class.getName());
  private final Map<String, List<String>> compUnitIdToMetricInJson;

  @Inject
  private MetricManagerMaster() {
    this.compUnitIdToMetricInJson = new HashMap<>();
  }

  @Override
  public synchronized void onMetricMessageReceived(final String metricKey, final String metricValue) {
    compUnitIdToMetricInJson.putIfAbsent(metricKey, new LinkedList<>());
    compUnitIdToMetricInJson.get(metricKey).add(metricValue);
    LOG.debug("{} / {}", metricKey, metricValue);
  }

  @Override
  public synchronized List<String> getMetricByKey(final String metricKey) {
    return compUnitIdToMetricInJson.getOrDefault(metricKey, Collections.emptyList());
  }
}
