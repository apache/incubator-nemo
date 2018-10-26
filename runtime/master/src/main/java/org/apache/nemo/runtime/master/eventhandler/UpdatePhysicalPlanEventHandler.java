package org.apache.nemo.runtime.master.eventhandler;

import org.apache.nemo.common.eventhandler.CompilerEventHandler;
import org.apache.nemo.runtime.common.eventhandler.UpdatePhysicalPlanEvent;
import org.apache.nemo.runtime.common.plan.PhysicalPlan;
import org.apache.nemo.runtime.master.scheduler.Scheduler;

import javax.inject.Inject;

/**
 * Class for handling event to update physical plan to the scheduler.
 */
public final class UpdatePhysicalPlanEventHandler implements CompilerEventHandler<UpdatePhysicalPlanEvent> {
  private Scheduler scheduler;

  @Inject
  private UpdatePhysicalPlanEventHandler() {
  }

  public void setScheduler(final Scheduler scheduler) {
    this.scheduler = scheduler;
  }

  @Override
  public Class<UpdatePhysicalPlanEvent> getEventClass() {
    return UpdatePhysicalPlanEvent.class;
  }

  @Override
  public void onNext(final UpdatePhysicalPlanEvent updatePhysicalPlanEvent) {
    final PhysicalPlan newPlan = updatePhysicalPlanEvent.getNewPhysicalPlan();

    this.scheduler.updatePlan(newPlan);
  }
}
