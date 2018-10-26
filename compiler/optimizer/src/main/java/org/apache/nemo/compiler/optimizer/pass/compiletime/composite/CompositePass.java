package org.apache.nemo.compiler.optimizer.pass.compiletime.composite;

import org.apache.nemo.common.dag.DAG;
import org.apache.nemo.common.ir.edge.IREdge;
import org.apache.nemo.common.ir.vertex.IRVertex;
import org.apache.nemo.common.ir.executionproperty.ExecutionProperty;
import org.apache.nemo.compiler.optimizer.pass.compiletime.CompileTimePass;
import org.apache.nemo.compiler.optimizer.pass.compiletime.annotating.AnnotatingPass;

import java.util.*;

/**
 * A compile-time pass composed of multiple compile-time passes, which each modifies an IR DAG.
 */
public abstract class CompositePass extends CompileTimePass {
  private final List<CompileTimePass> passList;
  private final Set<Class<? extends ExecutionProperty>> prerequisiteExecutionProperties;

  /**
   * Constructor.
   * @param passList list of compile time passes.
   */
  public CompositePass(final List<CompileTimePass> passList) {
    this.passList = passList;
    this.prerequisiteExecutionProperties = new HashSet<>();
    passList.forEach(pass -> prerequisiteExecutionProperties.addAll(pass.getPrerequisiteExecutionProperties()));
    passList.forEach(pass -> {
      if (pass instanceof AnnotatingPass) {
        prerequisiteExecutionProperties.removeAll(((AnnotatingPass) pass).getExecutionPropertiesToAnnotate());
      }
    });
  }

  /**
   * Getter for list of compile time passes.
   * @return the list of CompileTimePass.
   */
  public final List<CompileTimePass> getPassList() {
    return passList;
  }

  @Override
  public final DAG<IRVertex, IREdge> apply(final DAG<IRVertex, IREdge> irVertexIREdgeDAG) {
    return recursivelyApply(irVertexIREdgeDAG, getPassList().iterator());
  }

  /**
   * Recursively apply the give list of passes.
   * @param dag dag.
   * @param passIterator pass iterator.
   * @return dag.
   */
  private DAG<IRVertex, IREdge> recursivelyApply(final DAG<IRVertex, IREdge> dag,
                                                 final Iterator<CompileTimePass> passIterator) {
    if (passIterator.hasNext()) {
      return recursivelyApply(passIterator.next().apply(dag), passIterator);
    } else {
      return dag;
    }
  }

  public final Set<Class<? extends ExecutionProperty>> getPrerequisiteExecutionProperties() {
    return prerequisiteExecutionProperties;
  }
}
