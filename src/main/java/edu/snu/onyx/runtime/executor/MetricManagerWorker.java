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

import edu.snu.onyx.runtime.common.grpc.Metrics;
import edu.snu.onyx.runtime.common.metric.MetricMessageSender;
import edu.snu.onyx.runtime.exception.UnknownFailureCauseException;
import edu.snu.onyx.runtime.common.metric.parameter.MetricFlushPeriod;
import org.apache.reef.annotations.audience.EvaluatorSide;
import org.apache.reef.tang.annotations.Parameter;

import javax.inject.Inject;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Metric sender that periodically flushes the collected metrics to Driver.
 */
@EvaluatorSide
public final class MetricManagerWorker implements MetricMessageSender {
  private final ScheduledExecutorService scheduledExecutorService;
  private final BlockingQueue<Metrics.Metric> metricMessageQueue;
  private final AtomicBoolean closed;

  @Inject
  private MetricManagerWorker(@Parameter(MetricFlushPeriod.class) final long flushingPeriod,
                              final MasterRPC masterRPC) {
    this.scheduledExecutorService = Executors.newSingleThreadScheduledExecutor();
    this.metricMessageQueue = new LinkedBlockingQueue<>();
    this.closed = new AtomicBoolean(false);
    Runnable batchMetricMessages = () -> {
      while (!closed.get() && !metricMessageQueue.isEmpty()) {

        // Build batched metric messages
        int size = metricMessageQueue.size();

        final Metrics.MetricList.Builder metricList = Metrics.MetricList.newBuilder();

        for (int i = 0; i < size; i++) {
          final Metrics.Metric metric = metricMessageQueue.poll();
          metricList.addMetric(i, metric);
        }

        masterRPC.newMetricBlockingStub().reportMetrics(metricList.build());
      }
    };
    this.scheduledExecutorService.scheduleAtFixedRate(batchMetricMessages, 0,
                                                      flushingPeriod, TimeUnit.MILLISECONDS);
  }

  @Override
  public void send(final String metricKey, final String metricValue) {
    metricMessageQueue.add(Metrics.Metric.newBuilder().setMetricKey(metricKey).setMetricValue(metricValue).build());
  }

  @Override
  public void close() throws UnknownFailureCauseException {
    closed.set(true);
    scheduledExecutorService.shutdown();
  }
}
