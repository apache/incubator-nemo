package org.apache.nemo.compiler.optimizer.pass.compiletime.annotating;

import org.apache.nemo.common.dag.DAG;
import org.apache.nemo.common.ir.edge.IREdge;
import org.apache.nemo.common.ir.edge.executionproperty.MetricCollectionProperty;
import org.apache.nemo.common.ir.vertex.IRVertex;
import org.apache.nemo.common.ir.edge.executionproperty.PartitionerProperty;
import org.apache.nemo.common.ir.vertex.OperatorVertex;
import org.apache.nemo.common.ir.vertex.transform.AggregateMetricTransform;
import org.apache.nemo.compiler.optimizer.pass.compiletime.Requires;

import java.util.List;

/**
 * Transient resource pass for tagging edges with {@link PartitionerProperty}.
 */
@Annotates(PartitionerProperty.class)
@Requires(MetricCollectionProperty.class)
public final class SkewPartitionerPass extends AnnotatingPass {
  /**
   * Default constructor.
   */
  public SkewPartitionerPass() {
    super(SkewPartitionerPass.class);
  }

  @Override
  public DAG<IRVertex, IREdge> apply(final DAG<IRVertex, IREdge> dag) {
    dag.getVertices().forEach(v -> {
      if (v instanceof OperatorVertex
        && ((OperatorVertex) v).getTransform() instanceof AggregateMetricTransform) {
        final List<IREdge> outEdges = dag.getOutgoingEdgesOf(v);
        outEdges.forEach(edge -> {
          // double checking.
          if (MetricCollectionProperty.Value.DataSkewRuntimePass
            .equals(edge.getPropertyValue(MetricCollectionProperty.class).get())) {
            edge.setPropertyPermanently(PartitionerProperty.of(PartitionerProperty.Value.DataSkewHashPartitioner));
          }
        });
      }
    });
    return dag;
  }
}
