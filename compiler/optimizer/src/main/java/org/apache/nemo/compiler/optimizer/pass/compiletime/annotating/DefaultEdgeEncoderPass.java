package org.apache.nemo.compiler.optimizer.pass.compiletime.annotating;

import org.apache.nemo.common.coder.EncoderFactory;
import org.apache.nemo.common.dag.DAG;
import org.apache.nemo.common.ir.edge.IREdge;
import org.apache.nemo.common.ir.edge.executionproperty.EncoderProperty;
import org.apache.nemo.common.ir.vertex.IRVertex;

/**
 * Pass for initiating IREdge Encoder ExecutionProperty with default dummy coder.
 */
@Annotates(EncoderProperty.class)
public final class DefaultEdgeEncoderPass extends AnnotatingPass {

  private static final EncoderProperty DEFAULT_DECODER_PROPERTY =
      EncoderProperty.of(EncoderFactory.DUMMY_ENCODER_FACTORY);

  /**
   * Default constructor.
   */
  public DefaultEdgeEncoderPass() {
    super(DefaultEdgeEncoderPass.class);
  }

  @Override
  public DAG<IRVertex, IREdge> apply(final DAG<IRVertex, IREdge> dag) {
    dag.topologicalDo(irVertex ->
        dag.getIncomingEdgesOf(irVertex).forEach(irEdge -> {
          if (!irEdge.getPropertyValue(EncoderProperty.class).isPresent()) {
            irEdge.setProperty(DEFAULT_DECODER_PROPERTY);
          }
        }));
    return dag;
  }
}
