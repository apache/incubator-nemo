package org.apache.nemo.common.exception;

/**
 * PhysicalPlanGenerationException.
 * Thrown when any exception occurs during the conversion
 * from {@link org.apache.nemo.common.dag.DAG}
 * to {org.apache.nemo.runtime.common.plan.physical.PhysicalPlan}
 */
public final class PhysicalPlanGenerationException extends RuntimeException {
  /**
   * PhysicalPlanGenerationException.
   * @param message message
   */
  public PhysicalPlanGenerationException(final String message) {
    super(message);
  }

  /**
   * PhysicalPlanGenerationException.
   * @param e throwable cause of the exception.
   */
  public PhysicalPlanGenerationException(final Throwable e) {
    super(e);
  }
}
