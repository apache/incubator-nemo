/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.nemo.runtime.master.metric;

import org.apache.nemo.runtime.common.RuntimeIdManager;
import org.apache.nemo.runtime.common.comm.ControlMessage;
import org.apache.nemo.runtime.common.message.MessageEnvironment;
import org.apache.nemo.runtime.common.metric.Metric;
import org.apache.nemo.runtime.master.scheduler.ExecutorRegistry;
import org.apache.reef.annotations.audience.DriverSide;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;

/**
 * A default metric message handler.
 */
@DriverSide
public final class MetricManagerMaster implements MetricMessageHandler {

  private static final Logger LOG = LoggerFactory.getLogger(MetricManagerMaster.class.getName());
  private final MetricStore metricStore = MetricStore.getStore();
  private boolean isTerminated;
  private final ExecutorRegistry executorRegistry;

  @Inject
  private MetricManagerMaster(final ExecutorRegistry executorRegistry) {
    this.isTerminated = false;
    this.executorRegistry = executorRegistry;
  }

  public synchronized void sendMetricFlushRequest() {
    executorRegistry.viewExecutors(executors -> executors.forEach(executor -> {
      final ControlMessage.Message message = ControlMessage.Message.newBuilder()
        .setId(RuntimeIdManager.generateMessageId())
        .setListenerId(MessageEnvironment.EXECUTOR_MESSAGE_LISTENER_ID)
        .setType(ControlMessage.MessageType.RequestMetricFlush)
        .build();
      executor.sendControlMessage(message);
    }));
  }

  @Override
  public synchronized void onMetricMessageReceived(final String metricType,
                                                   final String metricId,
                                                   final String metricField,
                                                   final byte[] metricValue) {
    if (!isTerminated) {
      final Class<Metric> metricClass = metricStore.getMetricClassByName(metricType);
      // process metric message
      try {
        if (metricStore.getOrCreateMetric(metricClass, metricId).processMetricMessage(metricField, metricValue)) {
          metricStore.triggerBroadcast(metricClass, metricId);
        }
      } catch (final Exception e) {
        LOG.warn("Error when processing metric message for {}, {}, {}.", metricType, metricId, metricField);
      }
    }
  }

  @Override
  public synchronized void terminate() {
    isTerminated = true;
  }
}
