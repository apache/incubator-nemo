package edu.snu.vortex.compiler.frontend.beam.element;

public abstract class Element<T> {
  public final boolean isWatermark() {
    return getClass() == Watermark.class;
  }

  public Watermark<T> asWatermark() {
    return (Watermark<T>)this;
  }

  public Record<T> asRecord() {
    return (Record<T>)this;
  }
}
