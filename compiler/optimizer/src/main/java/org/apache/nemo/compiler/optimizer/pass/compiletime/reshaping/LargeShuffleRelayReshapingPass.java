package org.apache.nemo.compiler.optimizer.pass.compiletime.reshaping;

import org.apache.nemo.common.dag.DAG;
import org.apache.nemo.common.dag.DAGBuilder;
import org.apache.nemo.common.ir.edge.executionproperty.DecoderProperty;
import org.apache.nemo.common.ir.edge.executionproperty.EncoderProperty;
import org.apache.nemo.common.ir.edge.executionproperty.CommunicationPatternProperty;
import org.apache.nemo.common.ir.edge.IREdge;
import org.apache.nemo.common.ir.vertex.IRVertex;
import org.apache.nemo.common.ir.vertex.OperatorVertex;
import org.apache.nemo.common.ir.vertex.transform.RelayTransform;
import org.apache.nemo.compiler.optimizer.pass.compiletime.Requires;

/**
 * Pass to modify the DAG for a job to batch the disk seek.
 * It adds a {@link OperatorVertex} with {@link RelayTransform} before the vertices
 * receiving shuffle edges,
 * to merge the shuffled data in memory and write to the disk at once.
 */
@Requires(CommunicationPatternProperty.class)
public final class LargeShuffleRelayReshapingPass extends ReshapingPass {

  /**
   * Default constructor.
   */
  public LargeShuffleRelayReshapingPass() {
    super(LargeShuffleRelayReshapingPass.class);
  }

  @Override
  public DAG<IRVertex, IREdge> apply(final DAG<IRVertex, IREdge> dag) {
    final DAGBuilder<IRVertex, IREdge> builder = new DAGBuilder<>();
    dag.topologicalDo(v -> {
      builder.addVertex(v);
      // We care about OperatorVertices that have any incoming edge that
      // has Shuffle as data communication pattern.
      if (v instanceof OperatorVertex && dag.getIncomingEdgesOf(v).stream().anyMatch(irEdge ->
              CommunicationPatternProperty.Value.Shuffle
          .equals(irEdge.getPropertyValue(CommunicationPatternProperty.class).get()))) {
        dag.getIncomingEdgesOf(v).forEach(edge -> {
          if (CommunicationPatternProperty.Value.Shuffle
                .equals(edge.getPropertyValue(CommunicationPatternProperty.class).get())) {
            // Insert a merger vertex having transform that write received data immediately
            // before the vertex receiving shuffled data.
            final OperatorVertex iFileMergerVertex = new OperatorVertex(new RelayTransform());

            builder.addVertex(iFileMergerVertex);
            final IREdge newEdgeToMerger =
              new IREdge(CommunicationPatternProperty.Value.Shuffle, edge.getSrc(), iFileMergerVertex);
            edge.copyExecutionPropertiesTo(newEdgeToMerger);
            final IREdge newEdgeFromMerger = new IREdge(CommunicationPatternProperty.Value.OneToOne,
                iFileMergerVertex, v);
            newEdgeFromMerger.setProperty(EncoderProperty.of(edge.getPropertyValue(EncoderProperty.class).get()));
            newEdgeFromMerger.setProperty(DecoderProperty.of(edge.getPropertyValue(DecoderProperty.class).get()));
            builder.connectVertices(newEdgeToMerger);
            builder.connectVertices(newEdgeFromMerger);
          } else {
            builder.connectVertices(edge);
          }
        });
      } else { // Others are simply added to the builder.
        dag.getIncomingEdgesOf(v).forEach(builder::connectVertices);
      }
    });
    return builder.build();
  }
}
