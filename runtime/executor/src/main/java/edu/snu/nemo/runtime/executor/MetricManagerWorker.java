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
package edu.snu.nemo.runtime.executor;

import edu.snu.nemo.runtime.common.RuntimeIdGenerator;
import edu.snu.nemo.runtime.common.comm.ControlMessage;
import edu.snu.nemo.runtime.common.message.MessageEnvironment;
import edu.snu.nemo.common.exception.UnknownFailureCauseException;
import edu.snu.nemo.runtime.common.message.PersistentConnectionToMasterMap;
import edu.snu.nemo.runtime.common.metric.parameter.MetricFlushPeriod;
import org.apache.reef.annotations.audience.EvaluatorSide;
import org.apache.reef.tang.annotations.Parameter;

import javax.inject.Inject;
import java.util.concurrent.*;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Metric sender that periodically flushes the collected metrics to Driver.
 */
@EvaluatorSide
public final class MetricManagerWorker implements MetricMessageSender {

  private final ScheduledExecutorService scheduledExecutorService;
  private final BlockingQueue<ControlMessage.Metric> metricMessageQueue;
  private final PersistentConnectionToMasterMap persistentConnectionToMasterMap;

  private static final Logger LOG = LoggerFactory.getLogger(MetricManagerWorker.class.getName());

  @Inject
  private MetricManagerWorker(@Parameter(MetricFlushPeriod.class) final long flushingPeriod,
                              final PersistentConnectionToMasterMap persistentConnectionToMasterMap) {
    this.scheduledExecutorService = Executors.newSingleThreadScheduledExecutor();
    this.metricMessageQueue = new LinkedBlockingQueue<>();
    this.persistentConnectionToMasterMap = persistentConnectionToMasterMap;
    final Runnable batchMetricMessages = () -> flushMetricMessageQueueToMaster();
    this.scheduledExecutorService.scheduleAtFixedRate(batchMetricMessages, 0,
                                                      flushingPeriod, TimeUnit.MILLISECONDS);
  }

  private void flushMetricMessageQueueToMaster() {
    if (!metricMessageQueue.isEmpty()) {
      // Build batched metric messages
      int size = metricMessageQueue.size();

      final ControlMessage.MetricMsg.Builder metricMsgBuilder = ControlMessage.MetricMsg.newBuilder();

      LOG.debug("MetricManagerWorker Size: {}", size);
      for (int i = 0; i < size; i++) {
        final ControlMessage.Metric metric = metricMessageQueue.poll();
        LOG.debug("MetricManagerWorker addMetric: {}, {}, {}", size, i, metric);
        metricMsgBuilder.addMetric(i, metric);
      }

      persistentConnectionToMasterMap.getMessageSender(MessageEnvironment.RUNTIME_MASTER_MESSAGE_LISTENER_ID).send(
          ControlMessage.Message.newBuilder()
              .setId(RuntimeIdGenerator.generateMessageId())
              .setListenerId(MessageEnvironment.RUNTIME_MASTER_MESSAGE_LISTENER_ID)
              .setType(ControlMessage.MessageType.MetricMessageReceived)
              .setMetricMsg(metricMsgBuilder.build())
              .build());
    }
  }

  @Override
  public void send(final String metricKey, final String metricValue) {
    LOG.debug("Executor logged! {}", metricKey);
    metricMessageQueue.add(
        ControlMessage.Metric.newBuilder().setMetricKey(metricKey).setMetricValue(metricValue).build());
  }

  @Override
  public void close() throws UnknownFailureCauseException {
    LOG.info("Shutting down MetricManager ");
    scheduledExecutorService.shutdownNow();
    flushMetricMessageQueueToMaster();
  }
}
