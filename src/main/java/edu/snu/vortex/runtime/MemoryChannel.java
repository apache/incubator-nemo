package edu.snu.vortex.runtime;

/**
 * Remote calls
 * - Send ChannelReadyMessage to Master
 * - Send ReadRequestMessage to remote Executor
 * - Remote Executor: Send data
 * - Me: Receive data
 */
public class MemoryChannel implements Channel {
  Object data;

  @Override
  public void write(Object data) {
    this.data = data;
  }

  @Override
  public Object read() {
    return data;
  }
}
