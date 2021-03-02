package org.apache.nemo.runtime.lambdaexecutor;


import io.netty.channel.Channel;
import io.netty.channel.SimpleChannelInboundHandler;
import org.apache.nemo.offloading.common.OffloadingTransform;
import org.apache.nemo.offloading.common.TaskCaching;
import org.apache.nemo.runtime.executor.common.controlmessages.TaskControlMessage;

import java.util.Map;

public final class LambdaRuntimeContext implements OffloadingTransform.OffloadingContext {

  private final Map<String, OffloadingHandler.LambdaEventHandler> taskAndEventHandlerMap;
  private final OffloadingHandler.LambdaEventHandler lambdaEventHandler;
  private final boolean isSf;
  private final String newExecutorId;
  private final String nameServerAddr;
  private final int nameServerPort;
  private final Channel controlChannel;
  public final long throttleRate;
  public final boolean testing;
  public final Map<String, TaskCaching> stageTaskMap;
  public final int requestId;
  public final SimpleChannelInboundHandler<TaskControlMessage> handler;

  public LambdaRuntimeContext(
    final Map<String, OffloadingHandler.LambdaEventHandler> taskAndEventHandlerMap,
    final OffloadingHandler.LambdaEventHandler lambdaEventHandler,
    final boolean isSf,
    final String nameServerAddr,
    final int nameServerPort,
    final String newExecutorId,
    final Channel controlChannel,
    final long throttleRate,
    final boolean testing,
    final Map<String, TaskCaching> stageTaskMap,
    final int requestId,
    final SimpleChannelInboundHandler<TaskControlMessage> handler) {
    this.taskAndEventHandlerMap = taskAndEventHandlerMap;
    this.lambdaEventHandler = lambdaEventHandler;
    this.isSf = isSf;
    this.newExecutorId = newExecutorId;
    this.nameServerAddr = nameServerAddr;
    this.nameServerPort = nameServerPort;
    this.controlChannel = controlChannel;
    this.throttleRate = throttleRate;
    this.testing = testing;
    this.stageTaskMap = stageTaskMap;
    this.requestId = requestId;
    this.handler = handler;
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

  @Override
  public Channel getControlChannel() {
    return controlChannel;
  }
}
