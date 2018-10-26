package org.apache.nemo.compiler.optimizer.pass.compiletime.annotating;

import org.apache.nemo.common.ir.edge.IREdge;
import org.apache.nemo.common.ir.edge.executionproperty.DataStoreProperty;
import org.apache.nemo.common.ir.vertex.IRVertex;
import org.apache.nemo.common.dag.DAG;
import org.apache.nemo.compiler.optimizer.pass.compiletime.Requires;

import java.util.List;

/**
 * A pass to support Disaggregated Resources by tagging edges.
 * This pass handles the DataStore ExecutionProperty.
 */
@Annotates(DataStoreProperty.class)
@Requires(DataStoreProperty.class)
public final class DisaggregationEdgeDataStorePass extends AnnotatingPass {
  /**
   * Default constructor.
   */
  public DisaggregationEdgeDataStorePass() {
    super(DisaggregationEdgeDataStorePass.class);
  }

  @Override
  public DAG<IRVertex, IREdge> apply(final DAG<IRVertex, IREdge> dag) {
    dag.getVertices().forEach(vertex -> { // Initialize the DataStore of the DAG with GlusterFileStore.
      final List<IREdge> inEdges = dag.getIncomingEdgesOf(vertex);
      inEdges.forEach(edge ->
        edge.setPropertyPermanently(DataStoreProperty.of(DataStoreProperty.Value.GlusterFileStore)));
    });
    return dag;
  }
}
