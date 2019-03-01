package org.apache.nemo.compiler.frontend.beam.transform;

import org.apache.nemo.common.Triple;
import org.apache.nemo.common.eventhandler.OffloadingResultEvent;
import org.apache.nemo.common.ir.OutputCollector;
import org.apache.nemo.common.punctuation.Watermark;
import org.apache.nemo.offloading.common.EventHandler;
import org.apache.nemo.offloading.common.OffloadingDecoder;
import org.apache.nemo.offloading.common.OffloadingEncoder;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Map;

public final class StatelessOffloadingEventHandler implements EventHandler<OffloadingResultEvent> {

  private final Map<String, OutputCollector> vertexAndCollectorMap;

  public StatelessOffloadingEventHandler(final Map<String, OutputCollector> vertexAndCollectorMap) {
    this.vertexAndCollectorMap = vertexAndCollectorMap;
  }

  @Override
  public void onNext(OffloadingResultEvent msg) {
    for (final Triple<String, String, Object> triple : msg.data) {
      final Object elem = triple.third;
      final OutputCollector collector = vertexAndCollectorMap.get(triple.first);

      if (elem instanceof Watermark) {
        collector.emitWatermark((Watermark) elem);
      } else {
        collector.emit(elem);
      }
    }
  }
}
