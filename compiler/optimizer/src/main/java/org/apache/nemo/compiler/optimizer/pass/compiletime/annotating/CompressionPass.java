package org.apache.nemo.compiler.optimizer.pass.compiletime.annotating;

import org.apache.nemo.common.dag.DAG;
import org.apache.nemo.common.ir.edge.IREdge;
import org.apache.nemo.common.ir.edge.executionproperty.CompressionProperty;
import org.apache.nemo.common.ir.vertex.IRVertex;


/**
 * A pass for applying compression algorithm for data flowing between vertices.
 */
@Annotates(CompressionProperty.class)
public final class CompressionPass extends AnnotatingPass {
  private final CompressionProperty.Value compression;

  /**
   * Default constructor. Uses LZ4 as default.
   */
  public CompressionPass() {
    this(CompressionProperty.Value.LZ4);
  }

  /**
   * Constructor.
   * @param compression Compression to apply on edges.
   */
  public CompressionPass(final CompressionProperty.Value compression) {
    super(CompressionPass.class);
    this.compression = compression;
  }

  @Override
  public DAG<IRVertex, IREdge> apply(final DAG<IRVertex, IREdge> dag) {
    dag.topologicalDo(vertex -> dag.getIncomingEdgesOf(vertex).stream()
        .filter(edge -> !edge.getPropertyValue(CompressionProperty.class).isPresent())
        .forEach(edge -> edge.setProperty(CompressionProperty.of(compression))));

    return dag;
  }
}
