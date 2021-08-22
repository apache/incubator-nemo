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


  public WorkStealingPass() {
    super(WorkStealingPass.class);
  }

  @Override
  public IRDAG apply(IRDAG irdag) {
    irdag.topologicalDo(irVertex -> {
      if (irVertex instanceof OperatorVertex) {
        Transform transform = ((OperatorVertex) irVertex).getTransform();
        if (transform.toString().contains("work stealing")) {
          irVertex.setProperty(EnableWorkStealingExecutionProperty.of(true));
        } else {
          irVertex.setProperty(EnableWorkStealingExecutionProperty.of(false));
        }
      }
    });
    return irdag;
  }
}
