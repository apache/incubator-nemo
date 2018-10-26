package org.apache.nemo.compiler.optimizer.pass.compiletime.annotating;

import org.apache.nemo.common.dag.DAG;
import org.apache.nemo.common.ir.edge.IREdge;
import org.apache.nemo.common.ir.edge.executionproperty.DuplicateEdgeGroupProperty;
import org.apache.nemo.common.ir.edge.executionproperty.DuplicateEdgeGroupPropertyValue;
import org.apache.nemo.common.ir.vertex.IRVertex;

import java.util.HashMap;
import java.util.Optional;

/**
 * A pass for annotate duplicate data for each edge.
 */
@Annotates(DuplicateEdgeGroupProperty.class)
public final class DuplicateEdgeGroupSizePass extends AnnotatingPass {

  /**
   * Default constructor.
   */
  public DuplicateEdgeGroupSizePass() {
    super(DuplicateEdgeGroupSizePass.class);
  }

  @Override
  public DAG<IRVertex, IREdge> apply(final DAG<IRVertex, IREdge> dag) {
    final HashMap<String, Integer> groupIdToGroupSize = new HashMap<>();
    dag.topologicalDo(vertex -> dag.getIncomingEdgesOf(vertex)
        .forEach(e -> {
          final Optional<DuplicateEdgeGroupPropertyValue> duplicateEdgeGroupProperty =
              e.getPropertyValue(DuplicateEdgeGroupProperty.class);
          if (duplicateEdgeGroupProperty.isPresent()) {
            final String groupId = duplicateEdgeGroupProperty.get().getGroupId();
            final Integer currentCount = groupIdToGroupSize.getOrDefault(groupId, 0);
            groupIdToGroupSize.put(groupId, currentCount + 1);
          }
        }));

    dag.topologicalDo(vertex -> dag.getIncomingEdgesOf(vertex)
        .forEach(e -> {
          final Optional<DuplicateEdgeGroupPropertyValue> duplicateEdgeGroupProperty =
              e.getPropertyValue(DuplicateEdgeGroupProperty.class);
          if (duplicateEdgeGroupProperty.isPresent()) {
            final String groupId = duplicateEdgeGroupProperty.get().getGroupId();
            if (groupIdToGroupSize.containsKey(groupId)) {
              duplicateEdgeGroupProperty.get().setGroupSize(groupIdToGroupSize.get(groupId));
            }
          }
        }));

    return dag;
  }
}
