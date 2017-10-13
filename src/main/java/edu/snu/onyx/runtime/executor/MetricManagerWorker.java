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
package edu.snu.onyx.runtime.executor;

import edu.snu.onyx.runtime.common.RuntimeIdGenerator;
import edu.snu.onyx.runtime.common.comm.ControlMessage;
import edu.snu.onyx.runtime.common.message.MessageEnvironment;
import edu.snu.onyx.runtime.common.metric.MetricMessageSender;
import edu.snu.onyx.runtime.exception.UnknownFailureCauseException;
import org.apache.reef.annotations.audience.EvaluatorSide;

import javax.inject.Inject;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Metric sender that periodically flushes the collected metrics to Driver.
 */
@EvaluatorSide
public final class MetricManagerWorker implements MetricMessageSender {
  private static final Logger LOG = LoggerFactory.getLogger(MetricManagerWorker.class.getName());
  private final PersistentConnectionToMasterMap persistentConnectionToMasterMap;

  @Inject
  private MetricManagerWorker(final PersistentConnectionToMasterMap persistentConnectionToMasterMap) {
    this.persistentConnectionToMasterMap = persistentConnectionToMasterMap;
  }

  @Override
  public void send(final String metricKey, final String metricValue) {
    final ControlMessage.MetricMsg.Builder metricMsgBuilder = ControlMessage.MetricMsg.newBuilder();
    ControlMessage.Metric metric = ControlMessage.Metric.newBuilder()
        .setMetricKey(metricKey).setMetricValue(metricValue).build();
    metricMsgBuilder.addMetric(metric);

    persistentConnectionToMasterMap.getMessageSender(MessageEnvironment.RUNTIME_MASTER_MESSAGE_LISTENER_ID).send(
        ControlMessage.Message.newBuilder()
            .setId(RuntimeIdGenerator.generateMessageId())
            .setListenerId(MessageEnvironment.RUNTIME_MASTER_MESSAGE_LISTENER_ID)
            .setType(ControlMessage.MessageType.MetricMessageReceived)
            .setMetricMsg(metricMsgBuilder.build())
            .build());
  }

  @Override
  public void close() throws UnknownFailureCauseException {
  }
}
