package org.apache.nemo.runtime.executor.task.util;


import org.apache.nemo.common.ir.OutputCollector;
import org.apache.nemo.common.ir.vertex.transform.Transform;
import org.apache.nemo.common.punctuation.Watermark;

import java.io.Serializable;
import java.util.LinkedList;

/**
 * This transform does not emit watermark to OutputWriter
 * because OutputWriter currently does not support watermarks (TODO #245)
 * @param <T> type
 */
public final class StreamTransformNoEmit<T> implements Transform<T, T> {
  private OutputCollector<T> outputCollector;
  private final LinkedList<Serializable> emittedEvents;
  private final LinkedList<Watermark> emittedWatermarks;

  public StreamTransformNoEmit(final LinkedList<Watermark> emittedWatermarks,
                        final LinkedList<Serializable> emittedEvents) {
    this.emittedWatermarks = emittedWatermarks;
    this.emittedEvents = emittedEvents;
  }

  @Override
  public void prepare(final Context context, final OutputCollector<T> outputCollector) {
    this.outputCollector = outputCollector;
  }

  @Override
  public void onWatermark(Watermark watermark) {
    emittedWatermarks.add(watermark);
  }

  @Override
  public void onData(final Object element) {
    emittedEvents.add((Serializable) element);
    // outputCollector.emit((T) element);
  }

  @Override
  public void close() {
    // Do nothing.
  }
}

