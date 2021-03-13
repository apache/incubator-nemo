package org.apache.nemo.runtime.executor.common;

import org.apache.nemo.runtime.executor.common.monitoring.CpuBottleneckDetectorImpl;
import org.apache.reef.tang.annotations.DefaultImplementation;

@DefaultImplementation(CpuBottleneckDetectorImpl.class)
public interface CpuBottleneckDetector {

  void start();

  void close();
}
