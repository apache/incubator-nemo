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
package edu.snu.vortex.runtime.common.metric;

import edu.snu.vortex.runtime.common.RuntimeIdGenerator;
import edu.snu.vortex.runtime.common.comm.ControlMessage;
import edu.snu.vortex.runtime.common.message.MessageEnvironment;
import edu.snu.vortex.runtime.exception.UnknownFailureCauseException;
import edu.snu.vortex.runtime.executor.PersistentConnectionToMasterMap;
import edu.snu.vortex.runtime.common.metric.parameter.MetricFlushPeriod;
import org.apache.reef.tang.annotations.Parameter;

import javax.inject.Inject;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Metric sender that periodically flushes the collected metrics to Driver.
 */
public final class PeriodicMetricSender implements MetricSender {

  private final ScheduledExecutorService scheduledExecutorService;
  private final BlockingQueue<String> metricMessageQueue;
  private final AtomicBoolean closed;

  private static final Logger LOG = LoggerFactory.getLogger(PeriodicMetricSender.class.getName());

  @Inject
  private PeriodicMetricSender(@Parameter(MetricFlushPeriod.class) final long flushingPeriod,
                               final PersistentConnectionToMasterMap persistentConnectionToMasterMap) {
    this.scheduledExecutorService = Executors.newSingleThreadScheduledExecutor();
    this.metricMessageQueue = new LinkedBlockingQueue<>();
    this.closed = new AtomicBoolean(false);
    Runnable batchMetricMessages = () -> {

      while (!closed.get() && !metricMessageQueue.isEmpty()) {

        // Build batched metric messages
        int size = metricMessageQueue.size();

        final ControlMessage.MetricMsg.Builder metricMsgBuilder = ControlMessage.MetricMsg.newBuilder();

        for (int i = 0; i < size; i++) {
          final String metricMsg = metricMessageQueue.poll();
          metricMsgBuilder.setMetricMessages(i, metricMsg);
        }

        persistentConnectionToMasterMap.getMessageSender(MessageEnvironment.RUNTIME_MASTER_MESSAGE_LISTENER_ID).send(
            ControlMessage.Message.newBuilder()
                .setId(RuntimeIdGenerator.generateMessageId())
                .setListenerId(MessageEnvironment.RUNTIME_MASTER_MESSAGE_LISTENER_ID)
                .setType(ControlMessage.MessageType.MetricMessageReceived)
                .setMetricMsg(metricMsgBuilder.build())
                .build());
      }
    };
    this.scheduledExecutorService.scheduleAtFixedRate(batchMetricMessages, 0,
                                                      flushingPeriod, TimeUnit.MILLISECONDS);
  }

  @Override
  public void send(final String jsonStr) {
    metricMessageQueue.add(jsonStr);
  }

  @Override
  public void close() throws UnknownFailureCauseException {
    closed.set(true);
    scheduledExecutorService.shutdown();
  }
}
