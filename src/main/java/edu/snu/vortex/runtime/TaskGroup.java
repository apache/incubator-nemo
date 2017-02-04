package edu.snu.vortex.runtime;

import java.io.Serializable;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

public class TaskGroup implements Serializable {
  private static AtomicInteger generator = new AtomicInteger(1);

  private final String id;
  private List<Task> tasks;

  public TaskGroup(List<Task> tasks) {
    this.id = "TaskGroup-" + generator.getAndIncrement();
    this.tasks = tasks;
  }

  public List<Task> getTasks() {
    return tasks;
  }

  public String getId() {
    return id;
  }

  @Override
  public String toString() {
    return id + "_" + tasks.toString();
  }
}
