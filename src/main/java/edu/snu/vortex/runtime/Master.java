package edu.snu.vortex.runtime;

import java.util.*;

/**
 * Remote calls
 * - Send TaskGroup(List of tasks)
 * - Receive ChannelReadyMessage
 */
public class Master {
  private static Master master;
  private static TaskDAG taskDAG;

  private final Map<String, Integer> chanIdToExecutorId;

  final List<Executor> executors;
  int executorIndex;

  public Master(final TaskDAG taskDAG) {
    this.master = this;
    this.taskDAG = taskDAG;
    this.chanIdToExecutorId = new HashMap<>();
    this.executors = new ArrayList<>();
    for (int i = 0; i < 5; i++)
      this.executors.add(new Executor(this));
    this.executorIndex = 0;
  }

  /////////////////////////////// Scheduling

  public void executeJob() {
    final List<TaskGroup> initialTaskGroups = taskDAG.getSourceStage();
    initialTaskGroups.forEach(this::scheduleTaskGroup);
  }

  private void scheduleTaskGroup(final TaskGroup taskGroup) {
    // Round-robin executor pick
    final int selectedIndex = (executorIndex++) % executors.size();
    final Executor executor = executors.get(selectedIndex);
    taskGroup.getTasks().stream()
        .map(Task::getOutChans)
        .flatMap(List::stream)
        .forEach(chan -> chanIdToExecutorId.put(chan.getId(), selectedIndex));
    executor.executeTaskGroup(taskGroup); // Remote call
  }

  /////////////////////////////// Shuffle (Remote call)

  public static void onRemoteChannelReady(final String chanId) {
    final List<TaskGroup> consumers = taskDAG.getConsumers(chanId);
    consumers.forEach(group -> master.scheduleTaskGroup(group));
  }

  // Get executor where the channel data resides
  public static Optional<Executor> getExecutor(final String chanId) {
    System.out.println("Get Executor");
    System.out.println("chanIdToExecutorId: " + master.chanIdToExecutorId);
    System.out.println("chanId: " + chanId);
    final Integer index = master.chanIdToExecutorId.get(chanId);
    return index == null ? Optional.empty() : Optional.of(master.executors.get(index));
  }
}
