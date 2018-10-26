package org.apache.nemo.compiler.optimizer.pass.compiletime.annotating;

import org.apache.nemo.common.dag.DAG;
import org.apache.nemo.common.ir.edge.IREdge;
import org.apache.nemo.common.ir.vertex.IRVertex;
import org.apache.nemo.common.ir.vertex.executionproperty.ResourceSlotProperty;

/**
 * Sets {@link ResourceSlotProperty}.
 */
@Annotates(ResourceSlotProperty.class)
public final class ResourceSlotPass extends AnnotatingPass {

  public ResourceSlotPass() {
    super(ResourceSlotPass.class);
  }

  @Override
  public DAG<IRVertex, IREdge> apply(final DAG<IRVertex, IREdge> dag) {
    // On every vertex, if ResourceSlotProperty is not set, put it as true.
    dag.getVertices().stream()
        .filter(v -> !v.getPropertyValue(ResourceSlotProperty.class).isPresent())
        .forEach(v -> v.setProperty(ResourceSlotProperty.of(true)));
    return dag;
  }
}
