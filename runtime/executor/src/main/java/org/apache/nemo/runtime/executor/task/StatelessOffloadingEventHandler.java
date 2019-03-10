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

public final class StatelessOffloadingEventHandler implements EventHandler<OffloadingResultEvent> {
  private static final Logger LOG = LoggerFactory.getLogger(StatelessOffloadingEventHandler.class.getName());
  private final Map<String, Pair<OperatorMetricCollector, OutputCollector>> vertexAndCollectorMap;
  private final Map<String, NextIntraTaskOperatorInfo> operatorVertexMap;
  private final Map<String, OutputWriter> outputWriterMap;

  public StatelessOffloadingEventHandler(
    final Map<String, Pair<OperatorMetricCollector, OutputCollector>> vertexAndCollectorMap,
    final Map<String, NextIntraTaskOperatorInfo> operatorVertexMap,
    final Map<String, OutputWriter> outputWriterMap) {
    this.vertexAndCollectorMap = vertexAndCollectorMap;
    this.operatorVertexMap = operatorVertexMap;
    this.outputWriterMap = outputWriterMap;
  }

  @Override
  public void onNext(OffloadingResultEvent msg) {
    LOG.info("Result received: cnt {}", msg.data.size());
    // TODO: should be executed on one thread!
    // TODO: Handle OutputWriter

    for (final Triple<List<String>, String, Object> triple : msg.data) {
      final Object elem = triple.third;

      for (final String nextOpId : triple.first) {
        if (operatorVertexMap.containsKey(nextOpId)) {
          final NextIntraTaskOperatorInfo interOp = operatorVertexMap.get(nextOpId);
          final OutputCollector collector = vertexAndCollectorMap.get(nextOpId).right();

          //LOG.info("Emit data to {}, {}, {}, {}", nextOpId, interOp, collector, elem);

          if (elem instanceof Watermark) {
            interOp.getWatermarkManager().trackAndEmitWatermarks(interOp.getEdgeIndex(), (Watermark) elem);
          } else if (elem instanceof TimestampAndValue) {
            final TimestampAndValue tsv = (TimestampAndValue) elem;
            collector.setInputTimestamp(tsv.timestamp);
            interOp.getNextOperator().getTransform().onData(tsv.value);
          } else {
            throw new RuntimeException("Unknown type: " + elem);
          }
        } else {
          LOG.info("Emit to output writer {}", nextOpId);
          // this is for output writer
          final OutputWriter outputWriter = outputWriterMap.get(nextOpId);
          if (elem instanceof Watermark) {
            outputWriter.writeWatermark((Watermark) elem);
          } else if (elem instanceof TimestampAndValue) {
            outputWriter.write(elem);
          } else {
            throw new RuntimeException("Unknown type: " + elem);
          }
        }
      }
    }
  }
}
