package edu.snu.vortex.runtime.singlethread;

import edu.snu.vortex.runtime.TCPChannel;
import edu.snu.vortex.runtime.Task;
import edu.snu.vortex.runtime.TaskGroup;

import java.util.HashMap;
import java.util.List;

public class Executor {
  final Master master;
  final HashMap<String, TCPChannel> tcpChannelHashMap;

  public Executor(final Master master) {
    this.master = master;
    this.tcpChannelHashMap = new HashMap<>();
  }

  void executeTaskGroup(final TaskGroup taskGroup) {
    System.out.println("Executor execute stage: " + taskGroup);
    taskGroup.getTasks().stream()
        .map(Task::getOutChans)
        .flatMap(List::stream)
        .filter(chan -> chan instanceof TCPChannel)
        .forEach(chan -> tcpChannelHashMap.put(chan.getId(), (TCPChannel)chan));
    taskGroup.getTasks().forEach(t -> t.compute());
  }

  public List readData(final String chanId) {
    // send data remotely
    return tcpChannelHashMap.get(chanId).read();
  }
}
