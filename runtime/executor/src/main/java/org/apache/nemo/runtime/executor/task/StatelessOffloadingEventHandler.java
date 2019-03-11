package org.apache.nemo.runtime.executor.task;

import org.apache.nemo.common.Pair;
import org.apache.nemo.runtime.executor.common.NextIntraTaskOperatorInfo;
import org.apache.nemo.common.punctuation.TimestampAndValue;
import org.apache.nemo.runtime.executor.datatransfer.OutputWriter;
import org.apache.nemo.runtime.lambdaexecutor.Triple;
import org.apache.nemo.runtime.lambdaexecutor.OffloadingResultEvent;
import org.apache.nemo.common.ir.OutputCollector;
import org.apache.nemo.common.punctuation.Watermark;
import org.apache.nemo.offloading.common.EventHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentLinkedQueue;

public final class StatelessOffloadingEventHandler implements EventHandler<OffloadingResultEvent> {
  private static final Logger LOG = LoggerFactory.getLogger(StatelessOffloadingEventHandler.class.getName());

  private final ConcurrentLinkedQueue<OffloadingResultEvent> offloadingQueue;
  private final Map<String, Pair<OperatorMetricCollector, OutputCollector>> vertexAndCollectorMap;
  private final Map<String, NextIntraTaskOperatorInfo> operatorVertexMap;
  private final Map<String, OutputWriter> outputWriterMap;

  public StatelessOffloadingEventHandler(
    final ConcurrentLinkedQueue<OffloadingResultEvent> offloadingQueue,
    final Map<String, Pair<OperatorMetricCollector, OutputCollector>> vertexAndCollectorMap,
    final Map<String, NextIntraTaskOperatorInfo> operatorVertexMap,
    final Map<String, OutputWriter> outputWriterMap) {
    this.offloadingQueue = offloadingQueue;
    this.vertexAndCollectorMap = vertexAndCollectorMap;
    this.operatorVertexMap = operatorVertexMap;
    this.outputWriterMap = outputWriterMap;
  }

  @Override
  public void onNext(OffloadingResultEvent msg) {
    LOG.info("Result received: cnt {}", msg.data.size());
    offloadingQueue.add(msg);
  }
}
