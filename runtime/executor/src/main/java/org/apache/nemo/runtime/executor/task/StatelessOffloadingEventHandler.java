package org.apache.nemo.runtime.executor.task;

import org.apache.nemo.runtime.executor.common.NextIntraTaskOperatorInfo;
import org.apache.nemo.common.punctuation.TimestampAndValue;
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
  private final Map<String, OutputCollector> vertexAndCollectorMap;
  private final Map<String, NextIntraTaskOperatorInfo> operatorVertexMap;

  public StatelessOffloadingEventHandler(final Map<String, OutputCollector> vertexAndCollectorMap,
                                         final Map<String, NextIntraTaskOperatorInfo> operatorVertexMap) {
    this.vertexAndCollectorMap = vertexAndCollectorMap;
    this.operatorVertexMap = operatorVertexMap;
  }

  @Override
  public void onNext(OffloadingResultEvent msg) {
    LOG.info("Result received: cnt {}", msg.data.size());
    // TODO: should be executed on one thread!
    // TODO: Handle OutputWriter

    for (final Triple<List<String>, String, Object> triple : msg.data) {
      final Object elem = triple.third;

      for (final String nextOpId : triple.first) {
        final NextIntraTaskOperatorInfo interOp = operatorVertexMap.get(nextOpId);
        final OutputCollector collector = vertexAndCollectorMap.get(nextOpId);

        //LOG.info("Emit data to {}, {}, {}, {}", nextOpId, interOp, collector, elem);

        if (elem instanceof Watermark) {
          interOp.getWatermarkManager().trackAndEmitWatermarks(interOp.getEdgeIndex(), (Watermark) elem);
        } else if (elem instanceof TimestampAndValue) {
          final TimestampAndValue tsv = (TimestampAndValue) elem;
          collector.setInputTimestamp(tsv.timestamp);
          interOp.getNextOperator().getTransform().onData(tsv.value);
        }
      }
    }
  }
}
