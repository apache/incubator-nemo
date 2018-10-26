package org.apache.nemo.compiler.optimizer.pass.compiletime.annotating;

import org.apache.nemo.common.dag.DAG;
import org.apache.nemo.common.ir.edge.IREdge;
import org.apache.nemo.common.ir.edge.executionproperty.CommunicationPatternProperty;
import org.apache.nemo.common.ir.edge.executionproperty.CompressionProperty;
import org.apache.nemo.common.ir.edge.executionproperty.DecompressionProperty;
import org.apache.nemo.common.ir.vertex.IRVertex;
import org.apache.nemo.compiler.optimizer.pass.compiletime.Requires;

import java.util.List;

/**
 * A pass to support Sailfish-like shuffle by tagging edges.
 * This pass modifies the decoder property toward {@link org.apache.nemo.common.ir.vertex.transform.RelayTransform}
 * to read data as byte arrays.
 */
@Annotates(DecompressionProperty.class)
@Requires(CommunicationPatternProperty.class)
public final class LargeShuffleDecompressionPass extends AnnotatingPass {
  /**
   * Default constructor.
   */
  public LargeShuffleDecompressionPass() {
    super(LargeShuffleDecompressionPass.class);
  }

  @Override
  public DAG<IRVertex, IREdge> apply(final DAG<IRVertex, IREdge> dag) {
    dag.getVertices().forEach(vertex -> {
      final List<IREdge> inEdges = dag.getIncomingEdgesOf(vertex);
      inEdges.forEach(edge -> {
        if (edge.getPropertyValue(CommunicationPatternProperty.class).get()
            .equals(CommunicationPatternProperty.Value.Shuffle)) {
          edge.setPropertyPermanently(DecompressionProperty.of(CompressionProperty.Value.None));

          dag.getOutgoingEdgesOf(edge.getDst())
              .forEach(edgeFromRelay ->
                  edgeFromRelay.setPropertyPermanently(DecompressionProperty.of(CompressionProperty.Value.LZ4)));
        }
      });
    });
    return dag;
  }
}
