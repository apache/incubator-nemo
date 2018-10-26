package org.apache.nemo.compiler.optimizer.pass.compiletime.annotating;

import org.apache.nemo.common.ir.edge.IREdge;
import org.apache.nemo.common.ir.edge.executionproperty.CommunicationPatternProperty;
import org.apache.nemo.common.ir.edge.executionproperty.DataStoreProperty;
import org.apache.nemo.common.ir.vertex.IRVertex;
import org.apache.nemo.common.dag.DAG;
import org.apache.nemo.common.ir.vertex.executionproperty.ResourcePriorityProperty;
import org.apache.nemo.compiler.optimizer.pass.compiletime.Requires;

import java.util.List;

/**
 * Transient resource pass for tagging edges with DataStore ExecutionProperty.
 */
@Annotates(DataStoreProperty.class)
@Requires(ResourcePriorityProperty.class)
public final class TransientResourceDataStorePass extends AnnotatingPass {
  /**
   * Default constructor.
   */
  public TransientResourceDataStorePass() {
    super(TransientResourceDataStorePass.class);
  }

  @Override
  public DAG<IRVertex, IREdge> apply(final DAG<IRVertex, IREdge> dag) {
    dag.getVertices().forEach(vertex -> {
      final List<IREdge> inEdges = dag.getIncomingEdgesOf(vertex);
      if (!inEdges.isEmpty()) {
        inEdges.forEach(edge -> {
          if (fromTransientToReserved(edge) || fromReservedToTransient(edge)) {
            edge.setPropertyPermanently(DataStoreProperty.of(DataStoreProperty.Value.LocalFileStore));
          } else if (CommunicationPatternProperty.Value.OneToOne
              .equals(edge.getPropertyValue(CommunicationPatternProperty.class).get())) {
            edge.setPropertyPermanently(DataStoreProperty.of(DataStoreProperty.Value.MemoryStore));
          } else {
            edge.setPropertyPermanently(DataStoreProperty.of(DataStoreProperty.Value.LocalFileStore));
          }
        });
      }
    });
    return dag;
  }

  /**
   * checks if the edge is from transient container to a reserved container.
   * @param irEdge edge to check.
   * @return whether or not the edge satisfies the condition.
   */
  static boolean fromTransientToReserved(final IREdge irEdge) {
    return ResourcePriorityProperty.TRANSIENT
        .equals(irEdge.getSrc().getPropertyValue(ResourcePriorityProperty.class).get())
        && ResourcePriorityProperty.RESERVED
        .equals(irEdge.getDst().getPropertyValue(ResourcePriorityProperty.class).get());
  }

  /**
   * checks if the edge is from reserved container to a transient container.
   * @param irEdge edge to check.
   * @return whether or not the edge satisfies the condition.
   */
  static boolean fromReservedToTransient(final IREdge irEdge) {
    return ResourcePriorityProperty.RESERVED
        .equals(irEdge.getSrc().getPropertyValue(ResourcePriorityProperty.class).get())
        && ResourcePriorityProperty.TRANSIENT
        .equals(irEdge.getDst().getPropertyValue(ResourcePriorityProperty.class).get());
  }
}
