package org.apache.nemo.compiler.optimizer.pass.compiletime.annotating;

import org.apache.nemo.common.ir.IRDAG;
import org.apache.nemo.common.ir.vertex.OperatorVertex;
import org.apache.nemo.common.ir.vertex.executionproperty.EnableWorkStealingExecutionProperty;
import org.apache.nemo.common.ir.vertex.transform.Transform;

/**
 * Optimization pass for tagging parallelism execution property.
 */
@Annotates(EnableWorkStealingExecutionProperty.class)
public class WorkStealingPass extends AnnotatingPass{

  private final boolean enableWorkStealing;

  public WorkStealingPass(boolean enableWorkStealing) {
    super(WorkStealingPass.class);
    this.enableWorkStealing = enableWorkStealing;
  }

  @Override
  public IRDAG apply(IRDAG irdag) {
    irdag.topologicalDo(irVertex -> {
      if (irVertex instanceof OperatorVertex) {
        Transform transform = ((OperatorVertex) irVertex).getTransform();

        irVertex.setProperty(EnableWorkStealingExecutionProperty.of(true));
      }
    });
    return irdag;
  }
}
