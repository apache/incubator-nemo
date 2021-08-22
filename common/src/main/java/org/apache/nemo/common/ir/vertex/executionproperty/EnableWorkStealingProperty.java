package org.apache.nemo.common.ir.vertex.executionproperty;

import org.apache.nemo.common.ir.executionproperty.VertexExecutionProperty;

public class EnableWorkStealingProperty
  extends VertexExecutionProperty<String> {
  /**
   * Default constructor.
   *
   * @param value value of the VertexExecutionProperty.
   */
  public EnableWorkStealingProperty(String value) {
    super(value);
  }

  /**
   * Static method exposing the constructor.
   *
   * @param value value of the new execution property.
   * @return the newly created execution property.
   */
  public static EnableWorkStealingProperty of(final String value) {
    return new EnableWorkStealingProperty(value);
  }
}
