package org.apache.nemo.compiler.optimizer.pass.compiletime.annotating;

import org.apache.nemo.common.coder.DecoderFactory;
import org.apache.nemo.common.dag.DAG;
import org.apache.nemo.common.ir.edge.IREdge;
import org.apache.nemo.common.ir.edge.executionproperty.DecoderProperty;
import org.apache.nemo.common.ir.vertex.IRVertex;

/**
 * Pass for initiating IREdge Decoder ExecutionProperty with default dummy coder.
 */
@Annotates(DecoderProperty.class)
public final class DefaultEdgeDecoderPass extends AnnotatingPass {

  private static final DecoderProperty DEFAULT_DECODER_PROPERTY =
      DecoderProperty.of(DecoderFactory.DUMMY_DECODER_FACTORY);

  /**
   * Default constructor.
   */
  public DefaultEdgeDecoderPass() {
    super(DefaultEdgeDecoderPass.class);
  }

  @Override
  public DAG<IRVertex, IREdge> apply(final DAG<IRVertex, IREdge> dag) {
    dag.topologicalDo(irVertex ->
        dag.getIncomingEdgesOf(irVertex).forEach(irEdge -> {
          if (!irEdge.getPropertyValue(DecoderProperty.class).isPresent()) {
            irEdge.setProperty(DEFAULT_DECODER_PROPERTY);
          }
        }));
    return dag;
  }
}
