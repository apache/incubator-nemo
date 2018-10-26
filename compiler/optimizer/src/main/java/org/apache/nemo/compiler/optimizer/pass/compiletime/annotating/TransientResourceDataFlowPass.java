package org.apache.nemo.compiler.optimizer.pass.compiletime.annotating;

import org.apache.nemo.common.dag.DAG;
import org.apache.nemo.common.ir.edge.IREdge;
import org.apache.nemo.common.ir.edge.executionproperty.DataFlowProperty;
import org.apache.nemo.common.ir.vertex.IRVertex;
import org.apache.nemo.common.ir.vertex.executionproperty.ResourcePriorityProperty;
import org.apache.nemo.compiler.optimizer.pass.compiletime.Requires;

import java.util.List;

import static org.apache.nemo.compiler.optimizer.pass.compiletime.annotating.TransientResourceDataStorePass.fromTransientToReserved;

/**
 * Push from transient resources to reserved resources.
 */
@Annotates(DataFlowProperty.class)
@Requires(ResourcePriorityProperty.class)
public final class TransientResourceDataFlowPass extends AnnotatingPass {
  /**
   * Default constructor.
   */
  public TransientResourceDataFlowPass() {
    super(TransientResourceDataFlowPass.class);
  }

  @Override
  public DAG<IRVertex, IREdge> apply(final DAG<IRVertex, IREdge> dag) {
    dag.getVertices().forEach(vertex -> {
      final List<IREdge> inEdges = dag.getIncomingEdgesOf(vertex);
      if (!inEdges.isEmpty()) {
        inEdges.forEach(edge -> {
          if (fromTransientToReserved(edge)) {
            edge.setPropertyPermanently(DataFlowProperty.of(DataFlowProperty.Value.Push));
          } else {
            edge.setPropertyPermanently(DataFlowProperty.of(DataFlowProperty.Value.Pull));
          }
        });
      }
    });
    return dag;
  }
}
