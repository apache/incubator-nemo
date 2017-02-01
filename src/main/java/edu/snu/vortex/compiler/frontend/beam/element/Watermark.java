package edu.snu.vortex.compiler.frontend.beam.element;

import org.joda.time.Instant;

public class Watermark<T> extends Element<T> {
  private final Instant timestamp;

  public Watermark(final Instant timestampMs) {
    this.timestamp = timestampMs;
  }

  public Instant getTimestamp() {
    return timestamp;
  }

  @Override
  public String toString() {
    return "Watermark-" + timestamp;
  }
}
