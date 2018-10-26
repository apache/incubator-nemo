package org.apache.nemo.common.ir.edge.executionproperty;

import org.apache.nemo.common.ir.executionproperty.EdgeExecutionProperty;

/**
 * Data persistence ExecutionProperty.
 */
public final class DataPersistenceProperty extends EdgeExecutionProperty<DataPersistenceProperty.Value> {
  /**
   * Constructor.
   * @param value value of the execution property.
   */
  private DataPersistenceProperty(final Value value) {
    super(value);
  }

  /**
   * Static method exposing the constructor.
   * @param value value of the new execution property.
   * @return the newly created execution property.
   */
  public static DataPersistenceProperty of(final Value value) {
    return new DataPersistenceProperty(value);
  }

  /**
   * Possible options for the data persistence strategy.
   */
  public enum Value {
    Discard,
    Keep
  }
}
