package edu.snu.vortex.runtime;

import edu.snu.vortex.runtime.common.ExecutionPlan;

import java.util.ArrayList;
import java.util.List;

public class Master {
  final List<Executor> executors = new ArrayList<>();

  public void executeJob(final ExecutionPlan executionPlan) {
    for (int i = 0; i < 5; i++)
      executors.add(new Executor(this));

    // Convert into Tasks
    // Round-robin executor pick
    // executor.executeTask()
  }

  public void onTaskCompleted() {
    // executor.executeTask() <- make the receiver task read the data
  }

  public Executor getExecutor() {

  }
}
