package edu.snu.coral.common.ir.edge.executionproperty;

import edu.snu.coral.common.ir.executionproperty.ExecutionProperty;

/**
 * Invariant data ExecutionProperty. Use to indicate same data edge when unrolling loop vertex.
 */
public final class InvariantDataProperty extends ExecutionProperty<Integer> {
  /**
   * Constructor.
   * @param value value of the execution property.
   */
  private InvariantDataProperty(final Integer value) {
    super(Key.InvariantData, value);
  }

  /**
   * Static method exposing the constructor.
   * @param value value of the new execution property.
   * @return the newly created execution property.
   */
  public static InvariantDataProperty of(final Integer value) {
    return new InvariantDataProperty(value);
  }
}
