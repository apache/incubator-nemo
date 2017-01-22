package edu.snu.vortex.compiler.frontend.beam.element;

import org.apache.beam.sdk.util.WindowedValue;

public class Record<T> extends Element<T> {
  private final WindowedValue<T> windowedValue;

  public Record(final WindowedValue<T> windowedValue) {
    this.windowedValue = windowedValue;
  }

  public WindowedValue<T> getWindowedValue() {
    return windowedValue;
  }

  @Override
  public String toString() {
    return "Record-" + windowedValue;
  }
}
