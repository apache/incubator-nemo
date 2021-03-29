package org.apache.nemo.compiler.optimizer.pass.compiletime.reshaping;

import org.apache.nemo.common.ir.IRDAG;
import org.apache.nemo.common.ir.edge.IREdge;
import org.apache.nemo.common.ir.edge.executionproperty.CommunicationPatternProperty;
import org.apache.nemo.common.ir.vertex.IRVertex;
import org.apache.nemo.common.ir.vertex.OperatorVertex;
import org.apache.nemo.common.ir.vertex.utility.StreamVertex;
import org.apache.nemo.compiler.optimizer.pass.compiletime.Requires;

import java.util.List;

@Requires(CommunicationPatternProperty.class)
public final class StatefulReshapingPass extends ReshapingPass {

  /**
   * Default constructor.
   */
  public StatefulReshapingPass() {
    super(StatefulReshapingPass.class);
  }


  @Override
  public IRDAG apply(final IRDAG dag) {
    dag.topologicalDo(vertex -> {
      final List<IREdge> edges = dag.getOutgoingEdgesOf(vertex);

      // for conditional routing vertex
      if (edges.size() == 1 &&
        CommunicationPatternProperty.Value.Shuffle.equals(
          edges.get(0).getPropertyValue(CommunicationPatternProperty.class).get())
        && edges.get(0).getDst() instanceof OperatorVertex
        && ((OperatorVertex)edges.get(0).getDst()).getTransform().isGBKPartialTransform()) {

        // add conditional routing vertex
        final IRVertex dst = edges.get(0).getDst();
        dag.insertConditionalRouter(edges.get(0));
      }
    });

    return dag;
  }
}
