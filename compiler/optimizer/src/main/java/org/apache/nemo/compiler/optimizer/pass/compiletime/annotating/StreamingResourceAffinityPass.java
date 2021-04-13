package org.apache.nemo.compiler.optimizer.pass.compiletime.annotating;

import org.apache.nemo.common.ir.IRDAG;
import org.apache.nemo.common.ir.edge.IREdge;
import org.apache.nemo.common.ir.edge.RuntimeEdge;
import org.apache.nemo.common.ir.edge.executionproperty.AdditionalOutputTagProperty;
import org.apache.nemo.common.ir.edge.executionproperty.CommunicationPatternProperty;
import org.apache.nemo.common.ir.vertex.IRVertex;
import org.apache.nemo.common.ir.vertex.OperatorVertex;
import org.apache.nemo.common.ir.vertex.executionproperty.ResourcePriorityProperty;
import org.apache.nemo.common.ir.vertex.utility.ConditionalRouterVertex;

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

    // Set [Src->CR vertex] to SOURCE in order to schedule them in the same machine
    if (dag.getRootVertices().size() > 1) {
      throw new RuntimeException("Root vertex size > 1");
    }

    final IRVertex rootVertex = dag.getRootVertices().get(0);
    dag.getOutgoingEdgesOf(rootVertex)
      .stream()
      .filter(edge -> edge.getDst() instanceof ConditionalRouterVertex)
      .forEach(edge -> edge.getDst()
        .setPropertyPermanently(ResourcePriorityProperty.of(ResourcePriorityProperty.SOURCE)));

    dag.topologicalDo(vertex -> {
      if (vertex instanceof ConditionalRouterVertex) {
        if (!vertex.getPropertyValue(ResourcePriorityProperty.class)
          .get().equals(ResourcePriorityProperty.SOURCE)) {

          final List<IRVertex> verticesToAdd = new LinkedList<>();
          final List<IRVertex> stack = new LinkedList<>();

          final IREdge transientEdge = dag.getOutgoingEdgesOf(vertex).stream().filter(edge ->
            edge.getPropertyValue(AdditionalOutputTagProperty.class).isPresent())
            .findFirst().get();

          stack.add(transientEdge.getDst());
          while (!stack.isEmpty()) {
            final IRVertex s = ((LinkedList<IRVertex>) stack).poll();
            final List<IREdge> outEdges = dag.getOutgoingEdgesOf(s);
            for (final IREdge outEdge : outEdges) {
              if (!(outEdge.getDst() instanceof ConditionalRouterVertex)) {
                if (!verticesToAdd.contains(outEdge.getDst())) {
                    verticesToAdd.add(outEdge.getDst());
                    stack.add(outEdge.getDst());
                    outEdge.getDst().setProperty(ResourcePriorityProperty.of(ResourcePriorityProperty.LAMBDA));
                }
              }
            }
          }
        }
      }
    });
    return dag;
  }
}
