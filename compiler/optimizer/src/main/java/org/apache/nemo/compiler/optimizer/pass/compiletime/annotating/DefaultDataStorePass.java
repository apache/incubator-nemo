package org.apache.nemo.compiler.optimizer.pass.compiletime.annotating;

import org.apache.nemo.common.dag.DAG;
import org.apache.nemo.common.ir.edge.IREdge;
import org.apache.nemo.common.ir.edge.executionproperty.DataStoreProperty;
import org.apache.nemo.common.ir.vertex.IRVertex;

/**
 * Edge data store pass to process inter-stage memory store edges.
 */
@Annotates(DataStoreProperty.class)
public final class DefaultDataStorePass extends AnnotatingPass {
  /**
   * Default constructor.
   */
  public DefaultDataStorePass() {
    super(DefaultDataStorePass.class);
  }

  @Override
  public DAG<IRVertex, IREdge> apply(final DAG<IRVertex, IREdge> dag) {
    dag.getVertices().forEach(vertex -> {
      dag.getIncomingEdgesOf(vertex).stream()
          .filter(edge -> !edge.getPropertyValue(DataStoreProperty.class).isPresent())
          .forEach(edge -> edge.setProperty(
              DataStoreProperty.of(DataStoreProperty.Value.LocalFileStore)));
    });
    return dag;
  }
}
