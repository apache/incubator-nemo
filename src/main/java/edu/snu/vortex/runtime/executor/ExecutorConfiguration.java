package edu.snu.vortex.runtime.executor;

/**
 * Defines Executor related configurations.
 */
public final class ExecutorConfiguration {
  private int defaultExecutorNum;
  private int defaultExecutorCapacity;
  private int executorNumThreads;

  public ExecutorConfiguration() {
  }

  public ExecutorConfiguration(final int defaultExecutorNum,
                               final int defaultExecutorCapacity,
                               final int executorNumThreads) {
    this.defaultExecutorNum = defaultExecutorNum;
    this.defaultExecutorCapacity = defaultExecutorCapacity;
    this.executorNumThreads = executorNumThreads;
  }

  public int getDefaultExecutorNum() {
    return defaultExecutorNum;
  }

  public int getDefaultExecutorCapacity() {
    return defaultExecutorCapacity;
  }

  public int getExecutorNumThreads() {
    return executorNumThreads;
  }

  @Override
  public String toString() {
    final StringBuffer sb = new StringBuffer("ExecutorConfiguration{");
    sb.append("defaultExecutorNum=").append(defaultExecutorNum);
    sb.append(", defaultExecutorCapacity=").append(defaultExecutorCapacity);
    sb.append(", executorNumThreads=").append(executorNumThreads);
    sb.append('}');
    return sb.toString();
  }
}
