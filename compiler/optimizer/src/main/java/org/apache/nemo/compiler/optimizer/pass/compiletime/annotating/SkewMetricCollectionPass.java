package org.apache.nemo.compiler.optimizer.pass.compiletime.annotating;

import org.apache.nemo.common.dag.DAG;
import org.apache.nemo.common.ir.edge.IREdge;
import org.apache.nemo.common.ir.edge.executionproperty.CommunicationPatternProperty;
import org.apache.nemo.common.ir.vertex.IRVertex;
import org.apache.nemo.common.ir.edge.executionproperty.MetricCollectionProperty;
import org.apache.nemo.common.ir.vertex.OperatorVertex;
import org.apache.nemo.common.ir.vertex.transform.MetricCollectTransform;
import org.apache.nemo.compiler.optimizer.pass.compiletime.Requires;

/**
 * Pass to annotate the IR DAG for skew handling.
 *
 * It specifies the target of dynamic optimization for skew handling
 * by setting appropriate {@link MetricCollectionProperty} to
 * outgoing shuffle edges from vertices with {@link MetricCollectTransform}.
 */
@Annotates(MetricCollectionProperty.class)
@Requires(CommunicationPatternProperty.class)
public final class SkewMetricCollectionPass extends AnnotatingPass {
  /**
   * Default constructor.
   */
  public SkewMetricCollectionPass() {
    super(SkewMetricCollectionPass.class);
  }

  @Override
  public DAG<IRVertex, IREdge> apply(final DAG<IRVertex, IREdge> dag) {
    dag.topologicalDo(v -> {
      // we only care about metric collection vertices.
      if (v instanceof OperatorVertex
        && ((OperatorVertex) v).getTransform() instanceof MetricCollectTransform) {
        dag.getOutgoingEdgesOf(v).forEach(edge -> {
          // double checking.
          if (edge.getPropertyValue(CommunicationPatternProperty.class).get()
              .equals(CommunicationPatternProperty.Value.Shuffle)) {
            edge.setPropertyPermanently(MetricCollectionProperty.of(
                MetricCollectionProperty.Value.DataSkewRuntimePass));
          }
        });
      }
    });
    return dag;
  }
}
