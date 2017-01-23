package edu.snu.vortex.runtime;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

public class TCPChannel extends Channel {

  public TCPChannel() {
  }

  List data;

  @Override
  public void write(List data) {
    System.out.println(getId() + " TCP Channel WRITE: " + data);
    this.data = data;

    // remote call to master
    Master.onRemoteChannelReady(this.getId());
  }

  @Override
  public List read() {
    // Local hit
    if (data != null) {
      System.out.println(getId() + " TCP Channel Local READ: " + data);
      final List oldData = data;
      data = new ArrayList(0);
      return oldData;
    } else {
      // remote call to master
      final Optional<Executor> remoteExecutor = Master.getExecutor(getId());
      if (!remoteExecutor.isPresent()) {
        System.out.println(getId() + " TCP Channel Not Ready: ");
        return new ArrayList(0);
      } else {
        // remote call to executor
        final List data = remoteExecutor.get().readData(getId());
        System.out.println(getId() + " TCP Channel Remote READ: ");
        return data;
      }
    }
  }
}
