package org.apache.nemo.compiler.optimizer.pass.compiletime.annotating;

import org.apache.nemo.common.dag.DAG;
import org.apache.nemo.common.ir.edge.IREdge;
import org.apache.nemo.common.ir.edge.executionproperty.DataFlowProperty;
import org.apache.nemo.common.ir.vertex.IRVertex;
import org.apache.nemo.common.ir.vertex.executionproperty.ResourceSlotProperty;
import org.apache.nemo.compiler.optimizer.pass.compiletime.Requires;

/**
 * Sets {@link ResourceSlotProperty}.
 */
@Annotates(ResourceSlotProperty.class)
@Requires(DataFlowProperty.class)
public final class LargeShuffleResourceSlotPass extends AnnotatingPass {

  /**
   * Default constructor.
   */
  public LargeShuffleResourceSlotPass() {
    super(LargeShuffleResourceSlotPass.class);
  }

  @Override
  public DAG<IRVertex, IREdge> apply(final DAG<IRVertex, IREdge> dag) {
    // On every vertex that receive push edge, if ResourceSlotProperty is not set, put it as false.
    // For other vertices, if ResourceSlotProperty is not set, put it as true.
    dag.getVertices().stream()
        .filter(v -> !v.getPropertyValue(ResourceSlotProperty.class).isPresent())
        .forEach(v -> {
          if (dag.getIncomingEdgesOf(v).stream().anyMatch(
              e -> e.getPropertyValue(DataFlowProperty.class)
                  .orElseThrow(() -> new RuntimeException(String.format("DataFlowProperty for %s must be set",
                      e.getId()))).equals(DataFlowProperty.Value.Push))) {
            v.setPropertyPermanently(ResourceSlotProperty.of(false));
          } else {
            v.setPropertyPermanently(ResourceSlotProperty.of(true));
          }
        });
    return dag;
  }
}
