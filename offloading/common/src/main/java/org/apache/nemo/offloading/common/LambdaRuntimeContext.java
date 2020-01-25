package org.apache.nemo.offloading.common;


import java.util.Map;

public final class LambdaRuntimeContext implements OffloadingTransform.OffloadingContext {

  private final Map<String, OffloadingHandler.LambdaEventHandler> taskAndEventHandlerMap;
  private final OffloadingHandler.LambdaEventHandler lambdaEventHandler;
  private final boolean isSf;
  private final String newExecutorId;
  private final String nameServerAddr;
  private final int nameServerPort;

  public LambdaRuntimeContext(
    final Map<String, OffloadingHandler.LambdaEventHandler> taskAndEventHandlerMap,
    final OffloadingHandler.LambdaEventHandler lambdaEventHandler,
    final boolean isSf,
    final String nameServerAddr,
    final int nameServerPort,
    final String newExecutorId) {
    this.taskAndEventHandlerMap = taskAndEventHandlerMap;
    this.lambdaEventHandler = lambdaEventHandler;
    this.isSf = isSf;
    this.newExecutorId = newExecutorId;
    this.nameServerAddr = nameServerAddr;
    this.nameServerPort = nameServerPort;
  }


  public String getNewExecutorId() {
    return newExecutorId;
  }

  public String getNameServerAddr() {
    return nameServerAddr;
  }

  public int getNameServerPort() {
    return nameServerPort;
  }

  public boolean getIsSf() {
    return isSf;
  }

  public Map<String, OffloadingHandler.LambdaEventHandler> getTaskAndEventHandlerMap() {
    return taskAndEventHandlerMap;
  }

  public OffloadingHandler.LambdaEventHandler getLambdaEventHandler() {
    return lambdaEventHandler;
  }
}
