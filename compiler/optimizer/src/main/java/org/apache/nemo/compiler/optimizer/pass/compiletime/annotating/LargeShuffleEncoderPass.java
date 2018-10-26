package org.apache.nemo.compiler.optimizer.pass.compiletime.annotating;

import org.apache.nemo.common.coder.BytesEncoderFactory;
import org.apache.nemo.common.dag.DAG;
import org.apache.nemo.common.ir.edge.IREdge;
import org.apache.nemo.common.ir.edge.executionproperty.CommunicationPatternProperty;
import org.apache.nemo.common.ir.edge.executionproperty.EncoderProperty;
import org.apache.nemo.common.ir.vertex.IRVertex;
import org.apache.nemo.compiler.optimizer.pass.compiletime.Requires;

import java.util.List;

/**
 * A pass to optimize large shuffle by tagging edges.
 * This pass modifies the encoder property toward {@link org.apache.nemo.common.ir.vertex.transform.RelayTransform}
 * to write data as byte arrays.
 */
@Annotates(EncoderProperty.class)
@Requires(CommunicationPatternProperty.class)
public final class LargeShuffleEncoderPass extends AnnotatingPass {
  /**
   * Default constructor.
   */
  public LargeShuffleEncoderPass() {
    super(LargeShuffleEncoderPass.class);
  }

  @Override
  public DAG<IRVertex, IREdge> apply(final DAG<IRVertex, IREdge> dag) {
    dag.getVertices().forEach(vertex -> {
      final List<IREdge> inEdges = dag.getIncomingEdgesOf(vertex);
      inEdges.forEach(edge -> {
        if (edge.getPropertyValue(CommunicationPatternProperty.class).get()
            .equals(CommunicationPatternProperty.Value.Shuffle)) {
          dag.getOutgoingEdgesOf(edge.getDst())
              .forEach(edgeFromRelay ->
                  edgeFromRelay.setPropertyPermanently(EncoderProperty.of(BytesEncoderFactory.of())));
        }
      });
    });
    return dag;
  }
}
