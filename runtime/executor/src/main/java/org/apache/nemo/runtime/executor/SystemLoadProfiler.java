package org.apache.nemo.runtime.executor;

import com.sun.management.OperatingSystemMXBean;

import javax.inject.Inject;
import java.lang.management.ManagementFactory;

public final class SystemLoadProfiler {

  private final OperatingSystemMXBean operatingSystemMXBean;

  @Inject
  private SystemLoadProfiler() {
    this.operatingSystemMXBean =
      (OperatingSystemMXBean) ManagementFactory.getOperatingSystemMXBean();
  }

  public double getCpuLoad() {
    return operatingSystemMXBean.getProcessCpuLoad();
  }
}
