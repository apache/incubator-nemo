package org.apache.nemo.compiler.backend;

import org.apache.nemo.common.ir.edge.IREdge;
import org.apache.nemo.common.ir.vertex.IRVertex;
import org.apache.nemo.common.dag.DAG;
import org.apache.nemo.compiler.backend.nemo.NemoBackend;
import org.apache.reef.tang.annotations.DefaultImplementation;

/**
 * Interface for backend components.
 * @param <Plan> the physical execution plan to compile the DAG into.
 */
@DefaultImplementation(NemoBackend.class)
public interface Backend<Plan> {
  /**
   * Compiles a DAG to a physical execution plan.
   *
   * @param dag the DAG to compile.
   * @return the execution plan generated.
   * @throws Exception Exception on the way.
   */
  Plan compile(DAG<IRVertex, IREdge> dag) throws Exception;
}
