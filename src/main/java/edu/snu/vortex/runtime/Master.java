package edu.snu.vortex.runtime;

import java.util.*;

/**
 * Remote calls
 * - Send TaskGroup(List of tasks)
 * - Receive ChannelReadyMessage
 */
public class Master {
  final List<Executor> executors;
  int executorIndex;

  public Master() {
    executors = new ArrayList<>();
    for (int i = 0; i < 5; i++)
      executors.add(new Executor(this));
    executorIndex = 0;
  }

  /////////////////////////////// Scheduling

  public void executeJob(final TaskDAG taskDAG) {
    final List<TaskGroup> initialTaskGroups = taskDAG.getSourceStage();
    initialTaskGroups.forEach(this::scheduleTaskGroup);
  }

  private void scheduleTaskGroup(final TaskGroup taskGroup) {
    // Round-robin executor pick
    final Executor executor = pickExecutor();
    executor.executeTaskGroup(taskGroup); // Remote call
  }

  private Executor pickExecutor() {
    executorIndex++;
    return executors.get(executorIndex % executors.size());
  }

  /////////////////////////////// Shuffle (Remote call)

  public static void onRemoteChannelReady(final String chanId) {
    /*
    final RtStage nextStage = remoteChanToDstStage.get(chanId);
    executeStage(nextStage);
    */

    // scheduleTaskGroup (caching?)
    // executor.executeTask() <- make the receiver task read the data
  }

  // Get executor where the channel data resides
  public static Executor getExecutor(final String chanId) {
    return null;
  }
}
