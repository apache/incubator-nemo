package org.apache.nemo.runtime.executor.burstypolicy;

import org.apache.reef.tang.annotations.DefaultImplementation;

public interface TaskOffloadingPolicy {

  void triggerPolicy();

  void close();
}
