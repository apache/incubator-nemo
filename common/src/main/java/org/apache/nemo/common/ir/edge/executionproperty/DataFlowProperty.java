package org.apache.nemo.common.ir.edge.executionproperty;

import org.apache.nemo.common.ir.executionproperty.EdgeExecutionProperty;

/**
 * DataFlowModel ExecutionProperty.
 */
public final class DataFlowProperty extends EdgeExecutionProperty<DataFlowProperty.Value> {
  /**
   * Constructor.
   * @param value value of the execution property.
   */
  private DataFlowProperty(final Value value) {
    super(value);
  }

  /**
   * Static method exposing the constructor.
   * @param value value of the new execution property.
   * @return the newly created execution property.
   */
  public static DataFlowProperty of(final Value value) {
    return new DataFlowProperty(value);
  }

  /**
   * Possible values of DataFlowModel ExecutionProperty.
   */
  public enum Value {
    Pull,
    Push,
  }
}
