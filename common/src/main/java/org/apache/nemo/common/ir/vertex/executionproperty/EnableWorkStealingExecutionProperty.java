package org.apache.nemo.common.ir.vertex.executionproperty;

import org.apache.nemo.common.ir.executionproperty.VertexExecutionProperty;

public class EnableWorkStealingExecutionProperty extends VertexExecutionProperty<Boolean> {
  /**
   * Default constructor.
   *
   * @param value value of the VertexExecutionProperty.
   */
  public EnableWorkStealingExecutionProperty(Boolean value) {
    super(value);
  }

  /**
   * Static method exposing the constructor.
   *
   * @param value value of the new execution property.
   * @return the newly created execution property.
   */
  public static EnableDynamicTaskSizingProperty of(final Boolean value) {
    return new EnableDynamicTaskSizingProperty(value);
  }
}
