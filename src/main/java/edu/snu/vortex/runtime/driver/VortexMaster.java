package edu.snu.vortex.runtime.driver;

import edu.snu.vortex.compiler.backend.Backend;
import edu.snu.vortex.compiler.backend.vortex.VortexBackend;
import edu.snu.vortex.compiler.frontend.Frontend;
import edu.snu.vortex.compiler.frontend.beam.BeamFrontend;
import edu.snu.vortex.compiler.ir.DAG;
import edu.snu.vortex.compiler.optimizer.Optimizer;
import edu.snu.vortex.runtime.*;
import org.apache.commons.lang.StringUtils;
import org.apache.reef.driver.task.RunningTask;
import org.apache.reef.tang.annotations.Parameter;

import javax.inject.Inject;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

final class VortexMaster {

  private final Map<String, ExecutorRepresenter> executorMap;
  private final List<ExecutorRepresenter> exeucutorList;
  private final String[] userArguments;
  private final Map<String, String> outChannelIdToExecutorMap;
  private final Set<String> readyIdChannelSet;
  private final Set<String> scheduledTaskGroupIdSet;

  private int executorIndex;
  private TaskDAG taskDAG;

  @Inject
  private VortexMaster(@Parameter(Parameters.UserArguments.class) final String args) {
    this.executorMap = new ConcurrentHashMap<>();
    this.exeucutorList = Collections.synchronizedList(new ArrayList<>());
    this.userArguments = args.split(",");
    this.outChannelIdToExecutorMap = new ConcurrentHashMap<>();
    this.readyIdChannelSet = Collections.newSetFromMap(new ConcurrentHashMap<String, Boolean>());
    this.scheduledTaskGroupIdSet = Collections.newSetFromMap(new ConcurrentHashMap<String, Boolean>());
  }

  void launchJob() {
    try {
      /**
       * Step 1: Compile
       */
      System.out.println("##### VORTEX COMPILER (Frontend) #####");
      System.out.println("User arguments: " + StringUtils.join(userArguments, " "));
      final Frontend frontend = new BeamFrontend();
      final DAG dag = frontend.compile(userArguments); // TODO #30: Use Tang to Parse User Arguments
      System.out.println(dag);

      System.out.println("##### VORTEX COMPILER (Optimizer) #####");
      final Optimizer optimizer = new Optimizer();
      final DAG optimizedDAG = optimizer.optimize(dag); // TODO #31: Interfaces for Runtime Optimization
      System.out.println(optimizedDAG);

      // TODO #28: Implement VortexBackend
      System.out.println("##### VORTEX COMPILER (Backend) #####");
      final Backend backend = new VortexBackend();
      this.taskDAG = (TaskDAG) backend.compile(optimizedDAG);
      System.out.println(taskDAG);
      System.out.println();

      /**
       * Step 2: Execute
       */
      System.out.println();
      System.out.println("##### VORTEX Runtime #####");
      executeJob();
    } catch (final Exception e) {
      System.out.println(executorIndex + ", " + exeucutorList);
      throw new RuntimeException(e);
    }
  }

  private void executeJob() {
    final List<TaskGroup> initialTaskGroups = taskDAG.getSourceStage();
    initialTaskGroups.forEach(this::scheduleTaskGroup);
  }

  private void scheduleTaskGroup(final TaskGroup taskGroup) {
    if (scheduledTaskGroupIdSet.contains(taskGroup.getId())) {
      return;
    }

    scheduledTaskGroupIdSet.add(taskGroup.getId());
    // Round-robin executor pick
    final int selectedIndex = (executorIndex++) % exeucutorList.size();
    final ExecutorRepresenter executor = exeucutorList.get(selectedIndex);


    taskGroup.getTasks().stream()
        .map(Task::getOutChans)
        .flatMap(List::stream)
        .filter(chan -> chan instanceof TCPChannel)
        .forEach(chan -> outChannelIdToExecutorMap.put(chan.getId(), executor.getId()));

    executor.sendExecuteTaskGroup(taskGroup);
  }

  private void onRemoteChannelReady(final String chanId) {
    readyIdChannelSet.add(chanId);
    final List<TaskGroup> consumers = taskDAG.getConsumers(chanId);
    consumers.forEach(this::scheduleTaskGroup);
  }

  private void onReadRequest(final String requestExecoturId, final String chanId) {
    if (readyIdChannelSet.remove(chanId)) {
      if (chanId.equals("Channel-15")) {
        System.out.println(outChannelIdToExecutorMap);
      }
      final String executorId = outChannelIdToExecutorMap.get(chanId);
      executorMap.get(executorId).sendReadRequest(requestExecoturId, chanId);
    } else { // Channel is not ready now.
      executorMap.get(requestExecoturId).sendNotReadyResponse(chanId);
    }
  }

  void onNewExecutor(final RunningTask runningTask) {
    final ExecutorRepresenter executorRepresenter = new ExecutorRepresenter(runningTask);
    executorMap.put(runningTask.getId(), executorRepresenter);
    exeucutorList.add(executorRepresenter);
  }

  void onExecutorMessage(final VortexMessage vortexMessage) {
    switch (vortexMessage.getType()) {
      case RemoteChannelReady:
        onRemoteChannelReady((String) vortexMessage.getData());
        break;
      case ReadRequest:
        onReadRequest(vortexMessage.getExecutorId(), (String) vortexMessage.getData());
        break;
    }
  }
}
