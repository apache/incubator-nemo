package org.apache.nemo.runtime.common.optimizer.pass.runtime;

import org.apache.nemo.common.eventhandler.RuntimeEventHandler;
import org.apache.nemo.common.pass.Pass;
import org.apache.nemo.runtime.common.plan.PhysicalPlan;

import java.util.Set;
import java.util.function.BiFunction;

/**
 * Abstract class for dynamic optimization passes, for dynamically optimizing a physical plan.
 * It is a BiFunction that takes an original physical plan and metric data, to produce a new physical plan
 * after dynamic optimization.
 * @param <T> type of the metric data used for dynamic optimization.
 */
public abstract class RuntimePass<T> extends Pass
    implements BiFunction<PhysicalPlan, T, PhysicalPlan> {
  /**
   * @return the set of event handlers used with the runtime pass.
   */
  public abstract Set<Class<? extends RuntimeEventHandler>> getEventHandlerClasses();
}
