package org.apache.nemo.compiler.optimizer.pass.compiletime.annotating;

import org.apache.nemo.common.dag.DAG;
import org.apache.nemo.common.ir.edge.IREdge;
import org.apache.nemo.common.ir.vertex.IRVertex;
import org.apache.nemo.common.ir.vertex.executionproperty.ResourceLocalityProperty;

/**
 * Sets {@link ResourceLocalityProperty}.
 */
@Annotates(ResourceLocalityProperty.class)
public final class ResourceLocalityPass extends AnnotatingPass {

  public ResourceLocalityPass() {
    super(ResourceLocalityPass.class);
  }

  @Override
  public DAG<IRVertex, IREdge> apply(final DAG<IRVertex, IREdge> dag) {
    // On every vertex, if ResourceLocalityProperty is not set, put it as true.
    dag.getVertices().stream()
        .filter(v -> !v.getPropertyValue(ResourceLocalityProperty.class).isPresent())
        .forEach(v -> v.setProperty(ResourceLocalityProperty.of(true)));
    return dag;
  }
}
