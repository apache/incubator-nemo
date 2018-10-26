package org.apache.nemo.runtime.common.optimizer;

import org.apache.nemo.common.Pair;
import org.apache.nemo.runtime.common.optimizer.pass.runtime.DataSkewRuntimePass;
import org.apache.nemo.runtime.common.plan.PhysicalPlan;
import org.apache.nemo.runtime.common.plan.StageEdge;

import java.util.*;

/**
 * Runtime optimizer class.
 */
public final class RunTimeOptimizer {
  /**
   * Private constructor.
   */
  private RunTimeOptimizer() {
  }

  /**
   * Dynamic optimization method to process the dag with an appropriate pass, decided by the stats.
   *
   * @param originalPlan original physical execution plan.
   * @return the newly updated optimized physical plan.
   */
  public static synchronized PhysicalPlan dynamicOptimization(
          final PhysicalPlan originalPlan,
          final Object dynOptData,
          final StageEdge targetEdge) {
    // Data for dynamic optimization used in DataSkewRuntimePass
    // is a map of <hash value, partition size>.
    final PhysicalPlan physicalPlan =
      new DataSkewRuntimePass()
        .apply(originalPlan, Pair.of(targetEdge, (Map<Object, Long>) dynOptData));
    return physicalPlan;
  }
}
