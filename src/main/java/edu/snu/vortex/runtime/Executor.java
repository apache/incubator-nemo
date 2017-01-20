package edu.snu.vortex.runtime;

import java.util.List;

public class Executor {
  final Master master;
  // final HashMap dataMap;

  public Executor(final Master master) {
    this.master = master;
  }

  void executeTaskGroup(final List<Task> tasks) {
    tasks.forEach(t -> t.compute());
  }

  public Object readData() {
    // get channel from dataMap
    // read from the channel
    // deregisterRemoteOutChannel();
    // return the data
    return null;
  }
}
