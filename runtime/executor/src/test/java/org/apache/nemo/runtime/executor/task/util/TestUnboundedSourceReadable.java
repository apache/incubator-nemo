package org.apache.nemo.runtime.executor.task.util;

import org.apache.nemo.common.punctuation.TimestampAndValue;

import java.io.IOException;
import java.util.LinkedList;
import java.util.List;
import java.util.NoSuchElementException;
import org.apache.nemo.common.ir.Readable;

// This emulates unbounded source that throws NoSuchElementException
// It reads current data until middle point and throws NoSuchElementException at the middle point.
// It resumes the data reading after emitting a watermark, and finishes at the end of the data.
public final class TestUnboundedSourceReadable implements Readable {
  final List<EventOrWatermark> elements;

  private long currWatermark = 0;

  public TestUnboundedSourceReadable(final List<EventOrWatermark> events) {
    this.elements = events;
  }

  @Override
  public boolean isAvailable() {
    return !elements.isEmpty() && !(elements.size() == 1 && elements.get(0).isWatermark());
  }

  @Override
  public void prepare() {

  }

  @Override
  public Object readCurrent() throws NoSuchElementException {
    while (true) {
      final EventOrWatermark e = elements.remove(0);
      if (e.isWatermark()) {
        currWatermark = e.watermark;
      } else {
        return new TimestampAndValue<>(System.currentTimeMillis(), e.event);
      }
    }
  }

  @Override
  public long readWatermark() {
    return currWatermark;
  }

  @Override
  public boolean isFinished() {
    return false;
  }

  @Override
  public List<String> getLocations() throws Exception {
    return null;
  }

  @Override
  public void close() throws IOException {
  }
}
