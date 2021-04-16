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
import org.apache.nemo.common.ir.vertex.utility.StateMergerVertex;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.LinkedList;
import java.util.List;
import java.util.stream.Collectors;

@Annotates(ResourcePriorityProperty.class)
public final class StreamingResourceAffinityPass extends AnnotatingPass {
  private static final Logger LOG = LoggerFactory.getLogger(StreamingResourceAffinityPass.class.getName());

  private final boolean lambdaAffinity;

  /**
   * Constructor.
   */
  public StreamingResourceAffinityPass(final boolean lambdaAffinity) {
    super(StreamingResourceAffinityPass.class);
    this.lambdaAffinity = lambdaAffinity;
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

    if (lambdaAffinity) {
      dag.topologicalDo(vertex -> {
        if (vertex instanceof ConditionalRouterVertex || vertex instanceof StateMergerVertex) {

          final List<IRVertex> verticesToAdd = new LinkedList<>();
          final List<IRVertex> stack = new LinkedList<>();

          final IREdge transientEdge = dag.getOutgoingEdgesOf(vertex).stream().filter(edge ->
            edge.isTransientEdge())
            .findFirst().get();

          stack.add(transientEdge.getDst());

          while (!stack.isEmpty()) {
            final IRVertex s = ((LinkedList<IRVertex>) stack).poll();
            LOG.info("Resource priority set to Lambda for {}", s);
            s.setProperty(ResourcePriorityProperty.of(ResourcePriorityProperty.LAMBDA));

            final List<IREdge> outEdges = dag.getOutgoingEdgesOf(s);
            for (final IREdge outEdge : outEdges) {
              if (!(outEdge.getDst() instanceof ConditionalRouterVertex)
                && !(outEdge.getDst() instanceof StateMergerVertex)) {
                if (!verticesToAdd.contains(outEdge.getDst())) {
                  verticesToAdd.add(outEdge.getDst());
                  stack.add(outEdge.getDst());
                }
              }
            }
          }
        }
      });
    }
    return dag;
  }
}
