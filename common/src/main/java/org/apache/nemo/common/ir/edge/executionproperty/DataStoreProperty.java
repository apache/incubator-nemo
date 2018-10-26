package org.apache.nemo.common.ir.edge.executionproperty;

import org.apache.nemo.common.ir.executionproperty.EdgeExecutionProperty;

/**
 * DataStore ExecutionProperty.
 */
public final class DataStoreProperty extends EdgeExecutionProperty<DataStoreProperty.Value> {
  /**
   * Constructor.
   * @param value value of the execution property.
   */
  private DataStoreProperty(final Value value) {
    super(value);
  }

  /**
   * Static method exposing the constructor.
   * @param value value of the new execution property.
   * @return the newly created execution property.
   */
  public static DataStoreProperty of(final Value value) {
    return new DataStoreProperty(value);
  }

  /**
   * Possible values of DataStore ExecutionProperty.
   */
  public enum Value {
    MemoryStore,
    SerializedMemoryStore,
    LocalFileStore,
    GlusterFileStore
  }
}
