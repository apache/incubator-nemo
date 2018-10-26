package org.apache.nemo.common.pass;

import org.apache.nemo.common.dag.DAG;
import org.apache.nemo.common.ir.edge.IREdge;
import org.apache.nemo.common.ir.vertex.IRVertex;

import java.io.Serializable;
import java.util.function.Predicate;

/**
 * Abstract class for optimization passes. All passes basically extends this class.
 */
public abstract class Pass implements Serializable {
  private Predicate<DAG<IRVertex, IREdge>> condition;

  /**
   * Default constructor.
   */
  public Pass() {
    this((dag) -> true);
  }

  /**
   * Constructor.
   * @param condition condition under which to run the pass.
   */
  private Pass(final Predicate<DAG<IRVertex, IREdge>> condition) {
    this.condition = condition;
  }

  /**
   * Getter for the condition under which to apply the pass.
   * @return the condition under which to apply the pass.
   */
  public final Predicate<DAG<IRVertex, IREdge>> getCondition() {
    return this.condition;
  }

  /**
   * Add the condition to the existing condition to run the pass.
   * @param newCondition the new condition to add to the existing condition.
   * @return the condition with the new condition added.
   */
  public final Pass addCondition(final Predicate<DAG<IRVertex, IREdge>> newCondition) {
    this.condition = this.condition.and(newCondition);
    return this;
  }
}
