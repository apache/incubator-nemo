package org.apache.nemo.runtime.common.eventhandler;

import org.apache.nemo.common.eventhandler.RuntimeEvent;
import org.apache.nemo.runtime.common.plan.PhysicalPlan;
import org.apache.nemo.runtime.common.plan.StageEdge;

/**
 * An event for triggering dynamic optimization.
 */
public final class DynamicOptimizationEvent implements RuntimeEvent {
  private final PhysicalPlan physicalPlan;
  private final Object dynOptData;
  private final String taskId;
  private final String executorId;
  private final StageEdge targetEdge;

  /**
   * Default constructor.
   * @param physicalPlan physical plan to be optimized.
   * @param taskId id of the task which triggered the dynamic optimization.
   * @param executorId the id of executor which executes {@code taskId}
   */
  public DynamicOptimizationEvent(final PhysicalPlan physicalPlan,
                                  final Object dynOptData,
                                  final String taskId,
                                  final String executorId,
                                  final StageEdge targetEdge) {
    this.physicalPlan = physicalPlan;
    this.taskId = taskId;
    this.dynOptData = dynOptData;
    this.executorId = executorId;
    this.targetEdge = targetEdge;
  }

  /**
   * @return the physical plan to be optimized.
   */
  public PhysicalPlan getPhysicalPlan() {
    return this.physicalPlan;
  }

  /**
   * @return id of the task which triggered the dynamic optimization.
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

  public Object getDynOptData() {
    return this.dynOptData;
  }

  public StageEdge getTargetEdge() {
    return this.targetEdge;
  }
}
