package org.apache.nemo.compiler.optimizer.pass.compiletime.annotating;

import org.apache.nemo.common.dag.DAG;
import org.apache.nemo.common.ir.edge.IREdge;
import org.apache.nemo.common.ir.edge.executionproperty.CommunicationPatternProperty;
import org.apache.nemo.common.ir.vertex.IRVertex;
import org.apache.nemo.common.ir.vertex.executionproperty.ClonedSchedulingProperty;
import org.apache.nemo.compiler.optimizer.pass.compiletime.Requires;

/**
 * Set the ClonedScheduling property of source vertices, in an upfront manner.
 */
@Annotates(ClonedSchedulingProperty.class)
@Requires(CommunicationPatternProperty.class)
public final class UpfrontCloningPass extends AnnotatingPass {
  /**
   * Default constructor.
   */
  public UpfrontCloningPass() {
    super(UpfrontCloningPass.class);
  }

  @Override
  public DAG<IRVertex, IREdge> apply(final DAG<IRVertex, IREdge> dag) {
    dag.getVertices().stream()
        .filter(vertex -> dag.getIncomingEdgesOf(vertex.getId())
          .stream()
          // TODO #198: Handle Un-cloneable Beam Sink Operators
          // only shuffle receivers (for now... as particular Beam sink operators fail when cloned)
          .anyMatch(edge ->
            edge.getPropertyValue(CommunicationPatternProperty.class)
              .orElseThrow(() -> new IllegalStateException())
              .equals(CommunicationPatternProperty.Value.Shuffle))
          )
        .forEach(vertex -> vertex.setProperty(
          ClonedSchedulingProperty.of(new ClonedSchedulingProperty.CloneConf()))); // clone upfront, always
    return dag;
  }
}
