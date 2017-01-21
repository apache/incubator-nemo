package edu.snu.vortex.runtime;

public class Executor {
  final Master master;
  // final HashMap dataMap;

  public Executor(final Master master) {
    this.master = master;
  }

  void executeTaskGroup(final TaskGroup taskGroup) {
    System.out.println("Executor execute stage: " + taskGroup);
    taskGroup.getTasks().forEach(t -> t.compute());
  }

  public Object readData() {
    // get channel from dataMap
    // read from the channel
    // deregisterRemoteOutChannel();
    // return the data
    return null;
  }
}
