package org.apache.nemo.runtime.executor.common.monitoring;

import org.apache.nemo.common.MonitoringThread;
import org.apache.nemo.conf.EvalConf;
import org.apache.nemo.conf.JobConf;
import org.apache.nemo.runtime.executor.common.TaskExecutorMapWrapper;
import org.apache.nemo.runtime.executor.common.tasks.TaskExecutor;
import org.apache.reef.tang.annotations.Parameter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import java.util.Map;

public final class SystemLoadProfiler {
  private static final Logger LOG = LoggerFactory.getLogger(SystemLoadProfiler.class.getName());

  // private final OperatingSystemMXBean operatingSystemMXBean;
  private final Map<TaskExecutor, Boolean> taskExecutors;
  // private final ThreadMXBean threadMXBean;
  private final MonitoringThread monitoringThread;

  private final String executorId;


  @Inject
  private SystemLoadProfiler(final TaskExecutorMapWrapper wrapper,
                             @Parameter(JobConf.ExecutorId.class) final String executorId,
                             @Parameter(EvalConf.Ec2.class) final boolean ec2,
                             @Parameter(EvalConf.CpuLimit.class) final double cpuLimit) {
    // this.operatingSystemMXBean =
    //  (OperatingSystemMXBean) ManagementFactory.getOperatingSystemMXBean();
    this.taskExecutors = wrapper.getTaskExecutorMap();
    this.executorId = executorId;
    // this.threadMXBean = ManagementFactory.getThreadMXBean();

    if (ec2) {
      this.monitoringThread = new MonitoringThread(1000);
    } else {
      this.monitoringThread = new MonitoringThread(1000, cpuLimit);
    }
  }

  public double getAvgCpuLoad() {
    return monitoringThread.getAvarageUsagePerCPU();
  }

  public double getCpuLoad() {
    // return operatingSystemMXBean.getSystemCpuLoad();
    LOG.info("Average CPU Load: {} in {}", monitoringThread.getAvarageUsagePerCPU(), executorId);
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
