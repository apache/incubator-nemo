package org.apache.nemo.compiler.optimizer.pass.compiletime.annotating;

import org.apache.nemo.common.coder.BytesDecoderFactory;
import org.apache.nemo.common.dag.DAG;
import org.apache.nemo.common.ir.edge.IREdge;
import org.apache.nemo.common.ir.edge.executionproperty.CommunicationPatternProperty;
import org.apache.nemo.common.ir.edge.executionproperty.DecoderProperty;
import org.apache.nemo.common.ir.vertex.IRVertex;
import org.apache.nemo.compiler.optimizer.pass.compiletime.Requires;

import java.util.List;

/**
 * A pass to optimize large shuffle by tagging edges.
 * This pass modifies the decoder property toward {@link org.apache.nemo.common.ir.vertex.transform.RelayTransform}
 * to read data as byte arrays.
 */
@Annotates(DecoderProperty.class)
@Requires(CommunicationPatternProperty.class)
public final class LargeShuffleDecoderPass extends AnnotatingPass {
  /**
   * Default constructor.
   */
  public LargeShuffleDecoderPass() {
    super(LargeShuffleDecoderPass.class);
  }

  @Override
  public DAG<IRVertex, IREdge> apply(final DAG<IRVertex, IREdge> dag) {
    dag.getVertices().forEach(vertex -> {
      final List<IREdge> inEdges = dag.getIncomingEdgesOf(vertex);
      inEdges.forEach(edge -> {
        if (edge.getPropertyValue(CommunicationPatternProperty.class).get()
            .equals(CommunicationPatternProperty.Value.Shuffle)) {
          edge.setPropertyPermanently(DecoderProperty.of(BytesDecoderFactory.of()));
        }
      });
    });
    return dag;
  }
}
