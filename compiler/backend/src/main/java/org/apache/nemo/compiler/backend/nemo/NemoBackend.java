package org.apache.nemo.compiler.backend.nemo;

import org.apache.nemo.common.dag.DAG;
import org.apache.nemo.compiler.backend.Backend;
import org.apache.nemo.common.ir.edge.IREdge;
import org.apache.nemo.common.ir.vertex.IRVertex;
import org.apache.nemo.runtime.common.RuntimeIdManager;
import org.apache.nemo.runtime.common.plan.PhysicalPlan;
import org.apache.nemo.runtime.common.plan.PhysicalPlanGenerator;
import org.apache.nemo.runtime.common.plan.Stage;
import org.apache.nemo.runtime.common.plan.StageEdge;

import javax.inject.Inject;

/**
 * Backend component for Nemo Runtime.
 */
public final class NemoBackend implements Backend<PhysicalPlan> {

  private final PhysicalPlanGenerator physicalPlanGenerator;

  /**
   * Constructor.
   */
  @Inject
  private NemoBackend(final PhysicalPlanGenerator physicalPlanGenerator) {
    this.physicalPlanGenerator = physicalPlanGenerator;
  }

  /**
   * Compiles an IR DAG into a {@link PhysicalPlan} to be submitted to Runtime.
   *
   * @param irDAG the IR DAG to compile.
   * @return the execution plan to be submitted to Runtime.
   */
  public PhysicalPlan compile(final DAG<IRVertex, IREdge> irDAG) {

    final DAG<Stage, StageEdge> stageDAG = physicalPlanGenerator.apply(irDAG);
    return new PhysicalPlan(RuntimeIdManager.generatePhysicalPlanId(), stageDAG);
  }
}
