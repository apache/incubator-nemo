package edu.snu.vortex.runtime.master;

import edu.snu.vortex.runtime.executor.ExecutorConfiguration;

/**
 * Defines all configurations required in Runtime.
 */
public final class RuntimeConfiguration {
  private long defaultScheduleTimeout;
  private ExecutorConfiguration executorConfiguration;

  public RuntimeConfiguration() {
  }

  public RuntimeConfiguration(final long defaultScheduleTimeout,
                              final ExecutorConfiguration executorConfiguration) {
    this.defaultScheduleTimeout = defaultScheduleTimeout;
    this.executorConfiguration = executorConfiguration;
  }

  public long getDefaultScheduleTimeout() {
    return defaultScheduleTimeout;
  }

  public ExecutorConfiguration getExecutorConfiguration() {
    return executorConfiguration;
  }

  @Override
  public String toString() {
    final StringBuffer sb = new StringBuffer("RuntimeConfiguration{");
    sb.append("defaultScheduleTimeout=").append(defaultScheduleTimeout);
    sb.append(", executorConfiguration=").append(executorConfiguration);
    sb.append('}');
    return sb.toString();
  }
}
