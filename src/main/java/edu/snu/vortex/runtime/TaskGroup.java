package edu.snu.vortex.runtime;

import java.io.Serializable;
import java.util.List;

public class TaskGroup implements Serializable {
  private List<Task> tasks;

  public TaskGroup(List<Task> tasks) {
    this.tasks = tasks;
  }

  public List<Task> getTasks() {
    return tasks;
  }

  @Override
  public String toString() {
    return tasks.toString();
  }
}
