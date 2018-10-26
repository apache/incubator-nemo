package org.apache.nemo.compiler.optimizer.pass.compiletime.annotating;

import org.apache.nemo.common.dag.DAG;
import org.apache.nemo.common.ir.edge.IREdge;
import org.apache.nemo.common.ir.edge.executionproperty.CommunicationPatternProperty;
import org.apache.nemo.common.ir.edge.executionproperty.DataFlowProperty;
import org.apache.nemo.common.ir.vertex.IRVertex;
import org.apache.nemo.compiler.optimizer.pass.compiletime.Requires;

import java.util.List;

/**
 * A pass to optimize large shuffle by tagging edges.
 * This pass handles the DataFlowModel ExecutionProperty.
 */
@Annotates(DataFlowProperty.class)
@Requires(CommunicationPatternProperty.class)
public final class LargeShuffleDataFlowPass extends AnnotatingPass {
  /**
   * Default constructor.
   */
  public LargeShuffleDataFlowPass() {
    super(LargeShuffleDataFlowPass.class);
  }

  @Override
  public DAG<IRVertex, IREdge> apply(final DAG<IRVertex, IREdge> dag) {
    dag.getVertices().forEach(vertex -> {
      final List<IREdge> inEdges = dag.getIncomingEdgesOf(vertex);
      inEdges.forEach(edge -> {
        if (edge.getPropertyValue(CommunicationPatternProperty.class).get()
            .equals(CommunicationPatternProperty.Value.Shuffle)) {
          edge.setPropertyPermanently(DataFlowProperty.of(DataFlowProperty.Value.Push)); // Push to the merger vertex.
        } else {
          edge.setPropertyPermanently(DataFlowProperty.of(DataFlowProperty.Value.Pull));
        }
      });
    });
    return dag;
  }
}
