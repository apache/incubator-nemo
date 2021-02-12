package org.apache.nemo.runtime.executor.monitoring;

import com.sun.management.OperatingSystemMXBean;
import org.apache.nemo.runtime.executor.TaskExecutorMapWrapper;
import org.apache.nemo.runtime.executor.common.TaskExecutor;

import javax.inject.Inject;
import java.lang.management.ManagementFactory;
import java.lang.management.ThreadMXBean;
import java.util.HashMap;
import java.util.Map;

public final class SystemLoadProfiler {

  // private final OperatingSystemMXBean operatingSystemMXBean;
  private final Map<TaskExecutor, Boolean> taskExecutors;
  // private final ThreadMXBean threadMXBean;
  private final MonitoringThread monitoringThread;

  @Inject
  private SystemLoadProfiler(final TaskExecutorMapWrapper wrapper) {
    // this.operatingSystemMXBean =
    //  (OperatingSystemMXBean) ManagementFactory.getOperatingSystemMXBean();
    this.taskExecutors = wrapper.getTaskExecutorMap();
    // this.threadMXBean = ManagementFactory.getThreadMXBean();
    this.monitoringThread = new MonitoringThread(1000);
  }

  public double getCpuLoad() {
    // return operatingSystemMXBean.getSystemCpuLoad();
    return monitoringThread.getTotalUsage();
  }

  public Map<TaskExecutor, Long> getTaskExecutorCpuTimeMap() {
    /*
    final Map<TaskExecutor, Long> map = new HashMap<>();
    for (final TaskExecutor taskExecutor : taskExecutors.keySet()) {
      final long threadTime = threadMXBean.getThreadCpuTime(taskExecutor.getThreadId());
      map.put(taskExecutor, threadTime);
    }

    return map;
    */
    return null;
  }

  public void close() {
    monitoringThread.stopMonitor();
  }
}
