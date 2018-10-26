package org.apache.nemo.compiler.optimizer.pass.compiletime.annotating;

import org.apache.nemo.common.dag.DAG;
import org.apache.nemo.common.ir.edge.IREdge;
import org.apache.nemo.common.ir.edge.executionproperty.DataStoreProperty;
import org.apache.nemo.common.ir.edge.executionproperty.DataPersistenceProperty;
import org.apache.nemo.common.ir.vertex.IRVertex;
import org.apache.nemo.compiler.optimizer.pass.compiletime.Requires;

/**
 * Pass for initiating IREdge data persistence ExecutionProperty with default values.
 */
@Annotates(DataPersistenceProperty.class)
@Requires(DataStoreProperty.class)
public final class DefaultDataPersistencePass extends AnnotatingPass {

  /**
   * Default constructor.
   */
  public DefaultDataPersistencePass() {
    super(DefaultDataPersistencePass.class);
  }

  @Override
  public DAG<IRVertex, IREdge> apply(final DAG<IRVertex, IREdge> dag) {
    dag.topologicalDo(irVertex ->
        dag.getIncomingEdgesOf(irVertex).forEach(irEdge -> {
          if (!irEdge.getPropertyValue(DataPersistenceProperty.class).isPresent()) {
            final DataStoreProperty.Value dataStoreValue
                = irEdge.getPropertyValue(DataStoreProperty.class).get();
            if (DataStoreProperty.Value.MemoryStore.equals(dataStoreValue)
                || DataStoreProperty.Value.SerializedMemoryStore.equals(dataStoreValue)) {
              irEdge.setProperty(DataPersistenceProperty.of(DataPersistenceProperty.Value.Discard));
            } else {
              irEdge.setProperty(DataPersistenceProperty.of(DataPersistenceProperty.Value.Keep));
            }
          }
        }));
    return dag;
  }
}
