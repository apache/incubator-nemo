package edu.snu.vortex.compiler.frontend.beam.element;

import java.time.Instant;

public class Watermark<T> extends Element<T> {
  public static final Watermark MAX_WATERMARK = new Watermark(Long.MAX_VALUE);

  private final Instant timestamp;

  public Watermark(final long timestampMs) {
    this.timestamp = Instant.ofEpochMilli(timestampMs);
  }

  public Instant getTimestamp() {
    return timestamp;
  }

  @Override
  public String toString() {
    return "Watermark-" + timestamp;
  }
}
