package org.apache.nemo.compiler.optimizer.pass.compiletime.annotating;

import org.apache.nemo.common.dag.DAG;
import org.apache.nemo.common.ir.edge.IREdge;
import org.apache.nemo.common.ir.edge.executionproperty.DataFlowProperty;
import org.apache.nemo.common.ir.edge.executionproperty.DataPersistenceProperty;
import org.apache.nemo.common.ir.vertex.IRVertex;
import org.apache.nemo.compiler.optimizer.pass.compiletime.Requires;

/**
 * A pass to optimize large shuffle by tagging edges.
 * This pass handles the data persistence ExecutionProperty.
 */
@Annotates(DataPersistenceProperty.class)
@Requires(DataFlowProperty.class)
public final class LargeShuffleDataPersistencePass extends AnnotatingPass {

  /**
   * Default constructor.
   */
  public LargeShuffleDataPersistencePass() {
    super(LargeShuffleDataPersistencePass.class);
  }

  @Override
  public DAG<IRVertex, IREdge> apply(final DAG<IRVertex, IREdge> dag) {
    dag.topologicalDo(irVertex ->
        dag.getIncomingEdgesOf(irVertex).forEach(irEdge -> {
          final DataFlowProperty.Value dataFlowModel = irEdge.getPropertyValue(DataFlowProperty.class).get();
          if (DataFlowProperty.Value.Push.equals(dataFlowModel)) {
            irEdge.setPropertyPermanently(DataPersistenceProperty.of(DataPersistenceProperty.Value.Discard));
          }
        }));
    return dag;
  }
}
