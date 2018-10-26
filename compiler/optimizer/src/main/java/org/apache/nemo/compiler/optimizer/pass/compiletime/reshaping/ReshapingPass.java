package org.apache.nemo.compiler.optimizer.pass.compiletime.reshaping;

import org.apache.nemo.common.ir.executionproperty.ExecutionProperty;
import org.apache.nemo.compiler.optimizer.pass.compiletime.CompileTimePass;
import org.apache.nemo.compiler.optimizer.pass.compiletime.Requires;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

/**
 * A compile-time pass that reshapes the structure of the IR DAG.
 * It is ensured by the compiler that no execution properties are modified by a ReshapingPass.
 */
public abstract class ReshapingPass extends CompileTimePass {
  private final Set<Class<? extends ExecutionProperty>> prerequisiteExecutionProperties;

  /**
   * Constructor.
   * @param cls the reshaping pass class.
   */
  public ReshapingPass(final Class<? extends ReshapingPass> cls) {
    final Requires requires = cls.getAnnotation(Requires.class);
    this.prerequisiteExecutionProperties = requires == null
        ? new HashSet<>() : new HashSet<>(Arrays.asList(requires.value()));
  }

  public final Set<Class<? extends ExecutionProperty>> getPrerequisiteExecutionProperties() {
    return prerequisiteExecutionProperties;
  }
}
