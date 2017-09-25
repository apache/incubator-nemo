package edu.snu.vortex.compiler.ir.executionproperty.edge;

import edu.snu.vortex.compiler.ir.executionproperty.ExecutionProperty;

/**
 * WriteOptimization ExecutionProperty.
 * TODO #492: remove this class while modularizing data communication pattern.
 */
public final class WriteOptimizationProperty extends ExecutionProperty<String> {
  private WriteOptimizationProperty(final String value) {
    super(Key.WriteOptimization, value);
  }

  public static WriteOptimizationProperty of(final String value) {
    return new WriteOptimizationProperty(value);
  }

  public static final String IFILE_WRITE = "IFileWrite";
}
