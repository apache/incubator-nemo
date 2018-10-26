package org.apache.nemo.common.ir.edge.executionproperty;

import org.apache.nemo.common.ir.executionproperty.EdgeExecutionProperty;

import java.io.Serializable;

/**
 * Edges with this property fetch a broadcast variable.
 */
public final class BroadcastVariableIdProperty extends EdgeExecutionProperty<Serializable> {

  /**
   * Constructor.
   * @param value id.
   */
  private BroadcastVariableIdProperty(final Serializable value) {
    super(value);
  }

  /**
   * Static method exposing constructor.
   * @param value id.
   * @return the newly created execution property.
   */
  public static BroadcastVariableIdProperty of(final Serializable value) {
    return new BroadcastVariableIdProperty(value);
  }
}
