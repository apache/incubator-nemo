package org.apache.nemo.compiler.optimizer.pass.compiletime;

import org.apache.nemo.common.ir.edge.IREdge;
import org.apache.nemo.common.ir.vertex.IRVertex;
import org.apache.nemo.common.dag.DAG;
import org.apache.nemo.common.ir.executionproperty.ExecutionProperty;
import org.apache.nemo.common.pass.Pass;

import java.util.Set;
import java.util.function.Function;

/**
 * Abstract class for compile-time optimization passes that processes the DAG.
 * It is a function that takes an original DAG to produce a processed DAG, after an optimization.
 */
public abstract class CompileTimePass extends Pass implements Function<DAG<IRVertex, IREdge>, DAG<IRVertex, IREdge>> {
  /**
   * Getter for prerequisite execution properties.
   * @return set of prerequisite execution properties.
   */
  public abstract Set<Class<? extends ExecutionProperty>> getPrerequisiteExecutionProperties();
}
