package edu.snu.vortex.runtime;

import java.util.List;

/**
 * TCP Chan Remote calls
 * - Send ChannelReadyMessage to Master
 * - Send ReadRequestMessage to remote Executor
 * - Remote Executor: Send data
 * - Me: Receive data
 */
public class TCPChannel extends Channel {

  public TCPChannel() {
  }

  List data;

  @Override
  public void write(List data) {
    System.out.println("TCP Channel WRITE: " + data);
    this.data = data;
    // master.onRemoteChannelReady(this.getId());
  }

  @Override
  public List read() {
    System.out.println("TCP Channel READ: " + data);
    return this.data;
  }
}
