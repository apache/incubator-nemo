package org.apache.nemo.runtime.executor;

import com.sun.management.OperatingSystemMXBean;
import org.apache.nemo.runtime.executor.task.TaskExecutor;

import javax.inject.Inject;
import java.lang.management.ManagementFactory;
import java.lang.management.ThreadMXBean;
import java.util.HashMap;
import java.util.Map;

public final class SystemLoadProfiler {

  private final OperatingSystemMXBean operatingSystemMXBean;
  private final Map<TaskExecutor, Boolean> taskExecutors;
  private final ThreadMXBean threadMXBean;

  @Inject
  private SystemLoadProfiler(final TaskExecutorMapWrapper wrapper) {
    this.operatingSystemMXBean =
      (OperatingSystemMXBean) ManagementFactory.getOperatingSystemMXBean();
    this.taskExecutors = wrapper.taskExecutorMap;
    this.threadMXBean = ManagementFactory.getThreadMXBean();

  }

  public double getCpuLoad() {
    return operatingSystemMXBean.getProcessCpuLoad();
  }

  public Map<TaskExecutor, Long> getTaskExecutorCpuTimeMap() {
    final Map<TaskExecutor, Long> map = new HashMap<>();
    for (final TaskExecutor taskExecutor : taskExecutors.keySet()) {
      final long threadTime = threadMXBean.getThreadCpuTime(taskExecutor.getThreadId());
      map.put(taskExecutor, threadTime);
    }

    return map;
  }
}
