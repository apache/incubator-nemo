package org.apache.nemo.runtime.lambdaexecutor.general;

import org.apache.nemo.runtime.executor.common.OffloadingPreparer;

public final class NoOffloadingPreparer implements OffloadingPreparer {
  @Override
  public void prepare(String taskId, byte[] bytes) {

  }
}
