package edu.snu.vortex.compiler.ir.executionproperty.edge;

import edu.snu.vortex.compiler.ir.executionproperty.ExecutionProperty;

/**
 * IsSideInput ExecutionProperty.
 */
public final class IsSideInputProperty extends ExecutionProperty<Boolean> {
  private IsSideInputProperty(final Boolean value) {
    super(Key.IsSideInput, value);
  }

  public static IsSideInputProperty of(final Boolean value) {
    return new IsSideInputProperty(value);
  }
}
