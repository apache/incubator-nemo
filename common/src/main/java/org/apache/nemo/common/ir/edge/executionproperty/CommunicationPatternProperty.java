package org.apache.nemo.common.ir.edge.executionproperty;

import org.apache.nemo.common.ir.executionproperty.EdgeExecutionProperty;

// TODO #492: modularizing runtime components for data communication pattern.
/**
 * DataCommunicationPattern ExecutionProperty.
 */
public final class CommunicationPatternProperty
    extends EdgeExecutionProperty<CommunicationPatternProperty.Value> {
  /**
   * Constructor.
   * @param value value of the execution property.
   */
  private CommunicationPatternProperty(final Value value) {
    super(value);
  }

  /**
   * Static method exposing the constructor.
   * @param value value of the new execution property.
   * @return the newly created execution property.
   */
  public static CommunicationPatternProperty of(final Value value) {
    return new CommunicationPatternProperty(value);
  }

  /**
   * Possible values of DataCommunicationPattern ExecutionProperty.
   */
  public enum Value {
    OneToOne,
    BroadCast,
    Shuffle
  }
}
