package edu.snu.vortex.runtime;

import java.util.*;

public class TaskDAG {
  private final Map<TaskGroup, List<Channel>> id2inChans;
  private final Map<TaskGroup, List<Channel>> id2outChans;
  private final List<List<TaskGroup>> stages;

  public TaskDAG() {
    this.stages = new ArrayList<>();
    this.id2inChans = new HashMap<>();
    this.id2outChans = new HashMap<>();
  }

  public List<TaskGroup> getSourceStage() {
    return stages.get(0);
  }

  public void addStage(final List<TaskGroup> stage) {
    stages.add(stage);
  }

  public void connectTasks(final Task src, final Task dst, final Channel chan) {
    //id2inChans.putIfAbsent(dst, new ArrayList<>());
    //id2outChans.putIfAbsent(src, new ArrayList<>());

    id2inChans.get(dst).add(chan);
    id2outChans.get(src).add(chan);
  }

  /**
   * Gets the edges coming in to the given operator
   * @param chan
   * @return
   */
  public Optional<List<Channel>> getInChannelsOf(final Channel chan) {
    final List<Channel> inChannels = id2inChans.get(chan.getId());
    return inChannels == null ? Optional.empty() : Optional.of(inChannels);
  }

  /**
   * Gets the edges going out of the given operator
   * @param chan
   * @return
   */
  public Optional<List<Channel>> getOutChannelsOf(final Channel chan) {
    final List<Channel> outChannels = id2outChans.get(chan.getId());
    return outChannels == null ? Optional.empty() : Optional.of(outChannels);
  }
}
