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

import com.fasterxml.jackson.databind.ObjectMapper;
import edu.snu.vortex.client.JobConf;
import edu.snu.vortex.runtime.common.comm.ControlMessage;
import edu.snu.vortex.runtime.exception.JsonParseException;
import edu.snu.vortex.runtime.executor.PersistentConnectionToMaster;
import edu.snu.vortex.runtime.executor.metric.parameter.MetricFlushPeriod;
import org.apache.reef.tang.annotations.Parameter;

import javax.inject.Inject;
import java.util.Map;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Metric sender that periodically flushes the collected metrics to Driver.
 */
public final class PeriodicMetricSender implements MetricSender {

  private final ScheduledExecutorService scheduledExecutorService;
  private final BlockingQueue<String> metricMessageQueue;
  private final AtomicBoolean closed;
  private final ObjectMapper objectMapper;

  @Inject
  private PeriodicMetricSender(@Parameter(MetricFlushPeriod.class) final long flushingPeriod,
                               @Parameter(JobConf.ExecutorId.class) final String executorId,
                               final PersistentConnectionToMaster persistentConnectionToMaster) {
    this.scheduledExecutorService = Executors.newSingleThreadScheduledExecutor();
    this.metricMessageQueue = new LinkedBlockingQueue<>();
    this.closed = new AtomicBoolean(false);
    this.objectMapper = new ObjectMapper();
    this.scheduledExecutorService.scheduleAtFixedRate(() -> {
      while (!closed.get() || !metricMessageQueue.isEmpty()) {
        // Build batched metric messages
        int size = metricMessageQueue.size();
        final ControlMessage.MetricMsg.Builder metricMsgBuilder = ControlMessage.MetricMsg.newBuilder();

        for (int index = 0; index < size; index++) {
          final String metricMsg = metricMessageQueue.poll();
          metricMsgBuilder.setExecutorId(executorId);
          metricMsgBuilder.setMessages(index, metricMsg);
        }

        // Send msg
        final ControlMessage.Message.Builder msgBuilder = ControlMessage.Message.newBuilder();
        msgBuilder.setMetricMsg(metricMsgBuilder.build());
        persistentConnectionToMaster.getMessageSender().send(msgBuilder.build());
      }
    }, flushingPeriod, flushingPeriod, TimeUnit.MILLISECONDS);
  }

  @Override
  public void send(final Map<String, Object> jsonMetricData) {
    // Serialize to Json string
    try {
      final String jsonStr = objectMapper.writeValueAsString(jsonMetricData);
      metricMessageQueue.add(jsonStr);
    } catch (final Exception e) {
      throw new JsonParseException(e);
    }
  }

  @Override
  public void close() throws Exception {
    closed.set(true);
    scheduledExecutorService.shutdown();
  }
}
