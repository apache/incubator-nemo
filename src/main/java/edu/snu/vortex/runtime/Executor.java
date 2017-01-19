package edu.snu.vortex.runtime;

import java.util.HashMap;

public class Executor {
  final Master master;
  final HashMap dataMap;

  public Executor(final Master master) {
    this.master = master;
  }

  void executeTask(final Task task) {
    // If input is in another executor
    master.getExecutor().readData();

    // run task
    // send output by notifying the master
    master.onTaskCompleted();
  }

  public Object readData() {
  }
}
