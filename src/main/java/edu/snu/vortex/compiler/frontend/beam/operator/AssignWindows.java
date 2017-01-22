package edu.snu.vortex.compiler.frontend.beam.operator;

import edu.snu.vortex.compiler.frontend.beam.WindowAssignContext;
import edu.snu.vortex.compiler.frontend.beam.element.Element;
import edu.snu.vortex.compiler.frontend.beam.element.Record;
import edu.snu.vortex.compiler.ir.operator.Do;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.WindowFn;
import org.apache.beam.sdk.util.WindowedValue;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;

public class AssignWindows<T, W extends BoundedWindow> extends Do<Element<T>, Element<T>, Object> {

  private final WindowFn<T, W> windowFn;

  public AssignWindows(WindowFn<T, W> windowFn) {
    this.windowFn = windowFn;
  }

  @Override
  public Iterable<Element<T>> transform(final Iterable<Element<T>> input,
                                        final Map<Object, Object> broadcasted) {
    final List<Element<T>> outputs = new ArrayList<>();
    input.forEach(element -> {
      if (element.isWatermark()) {
        outputs.add(element);
      } else {
        final WindowedValue<T> wv = element.asRecord().getWindowedValue();
        try {
          final Collection<W> windows = windowFn.assignWindows(new WindowAssignContext<T, W>(windowFn, wv));
          for (W window : windows) {
            outputs.add(new Record<>(WindowedValue.of(wv.getValue(), wv.getTimestamp(), window, wv.getPane())));
          }
        } catch (Exception e) {
          throw new RuntimeException();
        }
      }
    });
    return outputs;
  }
}
