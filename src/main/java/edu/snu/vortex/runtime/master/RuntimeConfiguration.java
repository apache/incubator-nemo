package edu.snu.vortex.runtime.master;

import edu.snu.vortex.runtime.executor.ExecutorConfiguration;

/**
 * Defines all configurations required in Runtime.
 */
public final class RuntimeConfiguration {
  private ExecutorConfiguration executorConfiguration;

  public RuntimeConfiguration() {
  }

  public RuntimeConfiguration(final ExecutorConfiguration executorConfiguration) {
    this.executorConfiguration = executorConfiguration;
  }

  public ExecutorConfiguration getExecutorConfiguration() {
    return executorConfiguration;
  }

  @Override
  public String toString() {
    final StringBuffer sb = new StringBuffer("RuntimeConfiguration{");
    sb.append("executorConfiguration=").append(executorConfiguration);
    sb.append('}');
    return sb.toString();
  }
}
