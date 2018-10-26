package org.apache.nemo.compiler.frontend.beam.transform;

import org.apache.beam.runners.core.SideInputReader;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.nemo.common.ir.vertex.transform.Transform;

import javax.annotation.Nullable;
import java.util.Collection;

/**
 * A sideinput reader that reads/writes side input values to context.
 */
public final class BroadcastVariableSideInputReader implements SideInputReader {

  // Nemo context for storing/getting side inputs
  private final Transform.Context context;

  // The list of side inputs that we're handling
  private final Collection<PCollectionView<?>> sideInputs;

  BroadcastVariableSideInputReader(final Transform.Context context,
                                   final Collection<PCollectionView<?>> sideInputs) {
    this.context = context;
    this.sideInputs = sideInputs;
  }

  @Nullable
  @Override
  public <T> T get(final PCollectionView<T> view, final BoundedWindow window) {
    // TODO #216: implement side input and windowing
    return ((WindowedValue<T>) context.getBroadcastVariable(view)).getValue();
  }

  @Override
  public <T> boolean contains(final PCollectionView<T> view) {
    return sideInputs.contains(view);
  }

  @Override
  public boolean isEmpty() {
    return sideInputs.isEmpty();
  }
}
