package edu.snu.vortex.compiler.frontend.beam.element;

import org.apache.beam.sdk.transforms.windowing.BoundedWindow;

public class Watermark<T> extends Element<T> {
  private final BoundedWindow window;

  public Watermark(final BoundedWindow window) {
    this.window = window;
  }

  public BoundedWindow getWindowToTrigger() {
    return this.window;
  }

  @Override
  public String toString() {
    return "Watermark-" + window;
  }
}
