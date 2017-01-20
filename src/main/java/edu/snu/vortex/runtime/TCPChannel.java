package edu.snu.vortex.runtime;

import java.util.List;

/**
 * TCP Chan Remote calls
 * - Send ChannelReadyMessage to Master
 * - Send ReadRequestMessage to remote Executor
 * - Remote Executor: Send data
 * - Me: Receive data
 */
public class TCPChannel implements Channel {
  List data;

  @Override
  public void write(List data) {
    System.out.println("Channel WRITE: " + data);
    this.data = data;
  }

  @Override public List read() {
    System.out.println("Channel READ: " + data);
    return this.data;
  }
}
