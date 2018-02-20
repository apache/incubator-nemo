package edu.snu.coral.common.ir.edge.executionproperty;

import edu.snu.coral.common.Pair;
import edu.snu.coral.common.ir.executionproperty.ExecutionProperty;

/**
 * Invariant data ExecutionProperty. Use to indicate same data edge when unrolling loop vertex.
 */
public final class InvariantDataProperty extends ExecutionProperty<Pair<Boolean, String>> {
  /**
   * Constructor.
   * @param value value of the execution property.
   */
  private InvariantDataProperty(final Pair<Boolean, String> value) {
    super(Key.InvariantData, value);
  }

  /**
   * Static method exposing the constructor.
   * @param value value of the new execution property.
   * @return the newly created execution property.
   */
  public static InvariantDataProperty of(final Pair<Boolean, String> value) {
    return new InvariantDataProperty(value);
  }
}
