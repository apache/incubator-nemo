package edu.snu.vortex.compiler.frontend.beam;

import com.google.common.collect.Iterables;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.WindowFn;
import org.apache.beam.sdk.util.WindowedValue;
import org.joda.time.Instant;

public class WindowAssignContext<InputT, W extends BoundedWindow> extends WindowFn<InputT, W>.AssignContext {
  private final WindowedValue<InputT> value;

  public WindowAssignContext(WindowFn<InputT, W> fn, WindowedValue<InputT> value) {
    fn.super();
    this.value = value;
  }

  @Override
  public InputT element() {
    return value.getValue();
  }

  @Override
  public Instant timestamp() {
    return value.getTimestamp();
  }

  @Override
  public BoundedWindow window() {
    return Iterables.getOnlyElement(value.getWindows());
  }
}
