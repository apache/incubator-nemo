package org.apache.nemo.compiler.optimizer.pass.compiletime.annotating;

import org.apache.nemo.common.dag.DAG;
import org.apache.nemo.common.ir.edge.IREdge;
import org.apache.nemo.common.ir.edge.executionproperty.CompressionProperty;
import org.apache.nemo.common.ir.edge.executionproperty.DecompressionProperty;
import org.apache.nemo.common.ir.vertex.IRVertex;
import org.apache.nemo.compiler.optimizer.pass.compiletime.Requires;


/**
 * A pass for applying decompression algorithm for data flowing between vertices.
 * It always
 */
@Annotates(CompressionProperty.class)
@Requires(CompressionProperty.class)
public final class DecompressionPass extends AnnotatingPass {

  /**
   * Constructor.
   */
  public DecompressionPass() {
    super(DecompressionPass.class);
  }

  @Override
  public DAG<IRVertex, IREdge> apply(final DAG<IRVertex, IREdge> dag) {
    dag.topologicalDo(vertex -> dag.getIncomingEdgesOf(vertex).stream()
        // Find edges which have a compression property but not decompression property.
        .filter(edge -> edge.getPropertyValue(CompressionProperty.class).isPresent()
            && !edge.getPropertyValue(DecompressionProperty.class).isPresent())
        .forEach(edge -> edge.setProperty(DecompressionProperty.of(
            edge.getPropertyValue(CompressionProperty.class).get()))));

    return dag;
  }
}
