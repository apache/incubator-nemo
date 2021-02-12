package org.apache.nemo.runtime.executor.monitoring;

import com.sun.management.OperatingSystemMXBean;
import org.apache.nemo.conf.EvalConf;
import org.apache.nemo.runtime.executor.TaskExecutorMapWrapper;
import org.apache.nemo.runtime.executor.common.TaskExecutor;
import org.apache.reef.tang.annotations.Parameter;

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
  private SystemLoadProfiler(final TaskExecutorMapWrapper wrapper,
                             @Parameter(EvalConf.Ec2.class) final boolean ec2,
                             @Parameter(EvalConf.CpuLimit.class) final double cpuLimit) {
    // this.operatingSystemMXBean =
    //  (OperatingSystemMXBean) ManagementFactory.getOperatingSystemMXBean();
    this.taskExecutors = wrapper.getTaskExecutorMap();
    // this.threadMXBean = ManagementFactory.getThreadMXBean();

    if (ec2) {
      this.monitoringThread = new MonitoringThread(500);
    } else {
      this.monitoringThread = new MonitoringThread(500, cpuLimit);
    }
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
