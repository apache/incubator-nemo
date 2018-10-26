package org.apache.nemo.common.ir.edge.executionproperty;

import org.apache.nemo.common.ir.executionproperty.EdgeExecutionProperty;

/**
 * Additional Output Tag Execution Property for edge that contains tag for additional outputs.
 */
public final class AdditionalOutputTagProperty extends EdgeExecutionProperty<String> {

  /**
   * Constructor.
   * @param value tag id of additional input.
   */
  private AdditionalOutputTagProperty(final String value) {
    super(value);
  }

  /**
   * Static method exposing constructor.
   * @param value tag id of additional input.
   * @return the newly created execution property.
   */
  public static AdditionalOutputTagProperty of(final String value) {
    return new AdditionalOutputTagProperty(value);
  }
}
