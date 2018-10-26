package org.apache.nemo.runtime.common.eventhandler;

import org.apache.nemo.common.eventhandler.PubSubEventHandlerWrapper;
import org.apache.nemo.common.eventhandler.RuntimeEventHandler;
import org.apache.nemo.runtime.common.optimizer.RunTimeOptimizer;
import org.apache.nemo.runtime.common.plan.PhysicalPlan;
import org.apache.nemo.runtime.common.plan.StageEdge;
import org.apache.reef.wake.impl.PubSubEventHandler;

import javax.inject.Inject;

/**
 * Class for handling event to perform dynamic optimization.
 */
public final class DynamicOptimizationEventHandler implements RuntimeEventHandler<DynamicOptimizationEvent> {
  private final PubSubEventHandler pubSubEventHandler;

  /**
   * Constructor.
   * @param pubSubEventHandlerWrapper the wrapper of the global pubSubEventHandler.
   */
  @Inject
  private DynamicOptimizationEventHandler(final PubSubEventHandlerWrapper pubSubEventHandlerWrapper) {
    this.pubSubEventHandler = pubSubEventHandlerWrapper.getPubSubEventHandler();
  }

  @Override
  public Class<DynamicOptimizationEvent> getEventClass() {
    return DynamicOptimizationEvent.class;
  }

  @Override
  public void onNext(final DynamicOptimizationEvent dynamicOptimizationEvent) {
    final PhysicalPlan physicalPlan = dynamicOptimizationEvent.getPhysicalPlan();
    final Object dynOptData = dynamicOptimizationEvent.getDynOptData();
    final StageEdge targetEdge = dynamicOptimizationEvent.getTargetEdge();

    final PhysicalPlan newPlan = RunTimeOptimizer.dynamicOptimization(physicalPlan, dynOptData, targetEdge);

    pubSubEventHandler.onNext(new UpdatePhysicalPlanEvent(newPlan, dynamicOptimizationEvent.getTaskId(),
        dynamicOptimizationEvent.getExecutorId()));
  }
}
