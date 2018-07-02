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
package edu.snu.nemo.runtime.master;

import javax.inject.Inject;

import edu.snu.nemo.runtime.common.RuntimeIdGenerator;
import edu.snu.nemo.runtime.common.comm.ControlMessage;
import edu.snu.nemo.runtime.common.message.MessageEnvironment;
import edu.snu.nemo.runtime.master.scheduler.ExecutorRegistry;
import org.apache.reef.annotations.audience.DriverSide;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

/**
 * A default metric message handler.
 */
@DriverSide
public final class MetricManagerMaster implements MetricMessageHandler {

  private static final Logger LOG = LoggerFactory.getLogger(MetricManagerMaster.class.getName());
  private final Map<String, List<String>> compUnitIdToMetricInJson;
  private boolean isTerminated;
  private final ExecutorRegistry executorRegistry;

  @Inject
  private MetricManagerMaster(final ExecutorRegistry executorRegistry) {
    this.compUnitIdToMetricInJson = new HashMap<>();
    this.isTerminated = false;
    this.executorRegistry = executorRegistry;
  }

  public synchronized void sendMetricFlushRequest() {
    executorRegistry.viewExecutors(executors -> executors.forEach(executor -> {
      final ControlMessage.Message message = ControlMessage.Message.newBuilder()
          .setId(RuntimeIdGenerator.generateMessageId())
          .setListenerId(MessageEnvironment.EXECUTOR_MESSAGE_LISTENER_ID)
          .setType(ControlMessage.MessageType.RequestMetricFlush)
          .build();
      executor.sendControlMessage(message);
    }));
  }

  @Override
  public synchronized void onMetricMessageReceived(final String metricKey, final String metricValue) {
    if (!isTerminated) {
      compUnitIdToMetricInJson.putIfAbsent(metricKey, new LinkedList<>());
      compUnitIdToMetricInJson.get(metricKey).add(metricValue);
      LOG.debug("{\"computationUnitId\":\"{}\", \"metricList\":{}}", metricKey, metricValue);
    }
  }

  @Override
  public synchronized List<String> getMetricByKey(final String metricKey) {
    return compUnitIdToMetricInJson.get(metricKey);
  }

  @Override
  public synchronized void terminate() {
    compUnitIdToMetricInJson.forEach((compUnitId, metricList) ->
        LOG.info("{\"computationUnitId\":\"{}\", \"metricList\":{}}", compUnitId, metricList));
    compUnitIdToMetricInJson.clear();
    isTerminated = true;
  }
}
