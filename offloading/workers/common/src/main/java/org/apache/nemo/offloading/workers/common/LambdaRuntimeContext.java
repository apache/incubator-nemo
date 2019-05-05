package org.apache.nemo.offloading.workers.common;


import org.apache.nemo.offloading.common.OffloadingTransform;

import java.util.Map;

public final class LambdaRuntimeContext implements OffloadingTransform.OffloadingContext {

  private final Map<String, OffloadingHandler.LambdaEventHandler> taskAndEventHandlerMap;
  private final OffloadingHandler.LambdaEventHandler lambdaEventHandler;

  public LambdaRuntimeContext(
    final Map<String, OffloadingHandler.LambdaEventHandler> taskAndEventHandlerMap,
    final OffloadingHandler.LambdaEventHandler lambdaEventHandler) {
    this.taskAndEventHandlerMap = taskAndEventHandlerMap;
    this.lambdaEventHandler = lambdaEventHandler;
  }

  public Map<String, OffloadingHandler.LambdaEventHandler> getTaskAndEventHandlerMap() {
    return taskAndEventHandlerMap;
  }

  public OffloadingHandler.LambdaEventHandler getLambdaEventHandler() {
    return lambdaEventHandler;
  }
}
