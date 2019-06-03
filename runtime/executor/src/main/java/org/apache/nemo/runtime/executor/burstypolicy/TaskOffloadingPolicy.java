package org.apache.nemo.runtime.executor.burstypolicy;

import org.apache.reef.tang.annotations.DefaultImplementation;

@DefaultImplementation(MultiplicativeIncrementOffloadingPolicy.class)
public interface TaskOffloadingPolicy {

  void triggerPolicy();

  void close();
}
