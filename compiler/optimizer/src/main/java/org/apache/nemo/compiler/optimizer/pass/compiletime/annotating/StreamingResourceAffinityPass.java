package org.apache.nemo.compiler.optimizer.pass.compiletime.annotating;

import org.apache.nemo.common.ir.IRDAG;
import org.apache.nemo.common.ir.edge.executionproperty.CommunicationPatternProperty;
import org.apache.nemo.common.ir.vertex.IRVertex;
import org.apache.nemo.common.ir.vertex.OperatorVertex;
import org.apache.nemo.common.ir.vertex.executionproperty.ResourcePriorityProperty;

import java.util.LinkedList;
import java.util.List;
import java.util.stream.Collectors;

@Annotates(ResourcePriorityProperty.class)
public final class StreamingResourceAffinityPass extends AnnotatingPass {

  /**
   * Constructor.
   */
  public StreamingResourceAffinityPass() {
    super(StreamingResourceAffinityPass.class);
  }

  @Override
  public IRDAG apply(final IRDAG dag) {
    // On every vertex, if ResourceLocalityProperty is not set, put it as true.

    dag.getVertices().forEach(vertex -> {
      vertex.setProperty(ResourcePriorityProperty.of(ResourcePriorityProperty.COMPUTE));
    });


    /*
    dag.getVertices().forEach(vertex -> {

      if (vertex instanceof OperatorVertex) {
        if (((OperatorVertex) vertex).getTransform().toString().contains("kvToEvent")) {
          vertex.setProperty(ResourcePriorityProperty.of(ResourcePriorityProperty.SOURCE));

          dag.getAncestors(vertex.getId()).forEach(ancestor -> {
            ancestor.setProperty(ResourcePriorityProperty.of(ResourcePriorityProperty.SOURCE));
          });
        }
      }
    });
    */

    final List<IRVertex> sourceStages = new LinkedList<>();
    sourceStages.addAll(dag.getRootVertices());

    while (!sourceStages.isEmpty()) {
      final IRVertex srcStageVertex = ((LinkedList<IRVertex>) sourceStages).poll();
      srcStageVertex.setProperty(ResourcePriorityProperty.of(ResourcePriorityProperty.SOURCE));

      sourceStages.addAll(dag.getOutgoingEdgesOf(srcStageVertex)
        .stream().filter(edge -> edge.getPropertyValue(CommunicationPatternProperty.class)
          .get()
          .equals(CommunicationPatternProperty.Value.OneToOne))
        .map(edge -> edge.getDst())
        .collect(Collectors.toList()));
    }

    return dag;
  }
}
