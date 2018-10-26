package org.apache.nemo.runtime.common.eventhandler;

import org.apache.nemo.common.eventhandler.CompilerEvent;
import org.apache.nemo.runtime.common.plan.PhysicalPlan;

/**
 * An event for updating the physical plan in the scheduler.
 */
public final class UpdatePhysicalPlanEvent implements CompilerEvent {
  private final PhysicalPlan newPhysicalPlan;
  private final String taskId;
  private final String executorId;

  /**
   * Constructor.
   * @param newPhysicalPlan the newly optimized physical plan.
   * @param taskId id of the task which triggered the dynamic optimization.
   * @param executorId the id of executor which executes {@code taskId}
   */
  UpdatePhysicalPlanEvent(final PhysicalPlan newPhysicalPlan,
                          final String taskId,
                          final String executorId) {
    this.newPhysicalPlan = newPhysicalPlan;
    this.taskId = taskId;
    this.executorId = executorId;
  }

  /**
   * @return the updated, newly optimized physical plan.
   */
  public PhysicalPlan getNewPhysicalPlan() {
    return this.newPhysicalPlan;
  }

  /**
   * @return id of the task which triggered the dynamic optimization
   */
  public String getTaskId() {
    return taskId;
  }

  /**
   * @return id of the executor which triggered the dynamic optimization
   */
  public String getExecutorId() {
    return executorId;
  }
}
