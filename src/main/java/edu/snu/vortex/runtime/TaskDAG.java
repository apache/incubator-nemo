package edu.snu.vortex.runtime;

import java.util.*;

public class TaskDAG {
  private final List<List<TaskGroup>> stages;
  private final Map<String, List<TaskGroup>> chanToConsumers;

  public TaskDAG() {
    this.stages = new ArrayList<>();
    this.chanToConsumers = new HashMap<>();
  }

  public List<TaskGroup> getSourceStage() {
    return stages.get(0);
  }

  public void addStage(final List<TaskGroup> newStage) {
    System.out.println("NEW STAGE");
    System.out.println(newStage);

    stages.forEach(prevGroupList -> prevGroupList.forEach(prevGroup -> prevGroup.getTasks().forEach(prevTask -> {
      newStage.forEach(newGroup -> newGroup.getTasks().forEach(newTask -> {
        final List<Channel> prevOutChans = prevTask.getOutChans();
        final List<Channel> newInChans = newTask.getInChans();

        prevOutChans.forEach(chan -> {
          if (newInChans.contains(chan)) {
            chanToConsumers.putIfAbsent(chan.getId(), new ArrayList<>());
            final List<TaskGroup> consumers = chanToConsumers.get(chan.getId());
            if (!consumers.contains(newGroup))
              consumers.add(newGroup);
            System.out.println("chan2consumers: " + chanToConsumers);
          }
        });
      }));
    })));

    stages.add(newStage);
  }

  public List<TaskGroup> getConsumers(final String channelId) {
    System.out.println("channelId: " + channelId);
    return chanToConsumers.get(channelId);
  }
}
