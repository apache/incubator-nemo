package org.apache.nemo.runtime.executor;

import com.google.protobuf.ByteString;
import org.apache.nemo.runtime.common.RuntimeIdManager;
import org.apache.nemo.runtime.common.comm.ControlMessage;
import org.apache.nemo.runtime.common.message.MessageEnvironment;
import org.apache.nemo.common.exception.UnknownFailureCauseException;
import org.apache.nemo.runtime.common.message.PersistentConnectionToMasterMap;
import org.apache.reef.annotations.audience.EvaluatorSide;

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

  private static final int FLUSHING_PERIOD = 3000;
  private static final Logger LOG = LoggerFactory.getLogger(MetricManagerWorker.class.getName());

  @Inject
  private MetricManagerWorker(final PersistentConnectionToMasterMap persistentConnectionToMasterMap) {
    this.scheduledExecutorService = Executors.newSingleThreadScheduledExecutor();
    this.metricMessageQueue = new LinkedBlockingQueue<>();
    this.persistentConnectionToMasterMap = persistentConnectionToMasterMap;
    final Runnable batchMetricMessages = () -> flushMetricMessageQueueToMaster();
    this.scheduledExecutorService.scheduleAtFixedRate(batchMetricMessages, 0,
                                                      FLUSHING_PERIOD, TimeUnit.MILLISECONDS);
  }

  @Override
  public void flush() {
    flushMetricMessageQueueToMaster();
    persistentConnectionToMasterMap.getMessageSender(MessageEnvironment.RUNTIME_MASTER_MESSAGE_LISTENER_ID).send(
        ControlMessage.Message.newBuilder()
            .setId(RuntimeIdManager.generateMessageId())
            .setListenerId(MessageEnvironment.RUNTIME_MASTER_MESSAGE_LISTENER_ID)
            .setType(ControlMessage.MessageType.MetricFlushed)
            .build());
  }

  private synchronized void flushMetricMessageQueueToMaster() {
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
              .setId(RuntimeIdManager.generateMessageId())
              .setListenerId(MessageEnvironment.RUNTIME_MASTER_MESSAGE_LISTENER_ID)
              .setType(ControlMessage.MessageType.MetricMessageReceived)
              .setMetricMsg(metricMsgBuilder.build())
              .build());
    }
  }

  @Override
  public void send(final String metricType, final String metricId,
                   final String metricField, final byte[] metricValue) {
    metricMessageQueue.add(
        ControlMessage.Metric.newBuilder()
            .setMetricType(metricType)
            .setMetricId(metricId)
            .setMetricField(metricField)
            .setMetricValue(ByteString.copyFrom(metricValue))
            .build());
  }

  @Override
  public void close() throws UnknownFailureCauseException {
    scheduledExecutorService.shutdownNow();
    flushMetricMessageQueueToMaster();
  }
}
