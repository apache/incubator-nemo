package edu.snu.vortex.runtime;

import java.io.Serializable;

public final class VortexMessage implements Serializable {
  private String executorId;
  private String targetExecutorId;
  private Type type;
  private Serializable data;

  public VortexMessage(String executorId, Type type, Serializable data) {
    this(executorId, null, type, data);
  }

  public VortexMessage(String executorId, String targetExecutorId, Type type, Serializable data) {
    this.executorId = executorId;
    this.targetExecutorId = targetExecutorId;
    this.type = type;
    this.data = data;
  }

  public String getExecutorId() {
    return executorId;
  }

  public String getTargetExecutorId() {
    return targetExecutorId;
  }

  public Type getType() {
    return type;
  }

  public Serializable getData() {
    return data;
  }

  public enum Type {
    // Master to Executor
    ExecuteTaskGroup,
    ExecutedCachedTaskGroup,
    ChannelNotReady,

    // Executor to Master
    RemoteChannelReady,


    // Executor to Master and Master to Executor (Master dispatch)
    ReadRequest,
  }
}
