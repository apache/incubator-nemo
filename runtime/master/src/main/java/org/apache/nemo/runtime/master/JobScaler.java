package org.apache.nemo.runtime.master;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.ByteBufOutputStream;
import org.apache.commons.lang3.tuple.Triple;
import org.apache.nemo.common.*;
import org.apache.nemo.common.coder.FSTSingleton;
import org.apache.nemo.common.exception.IllegalMessageException;
import org.apache.nemo.common.ir.vertex.executionproperty.ResourcePriorityProperty;
import org.apache.nemo.conf.EvalConf;
import org.apache.nemo.offloading.common.OffloadingMasterEvent;
import org.apache.nemo.runtime.common.comm.ControlMessage;
import org.apache.nemo.runtime.common.message.MessageContext;
import org.apache.nemo.runtime.common.message.MessageEnvironment;
import org.apache.nemo.runtime.common.message.MessageListener;
import org.apache.nemo.runtime.master.resource.ExecutorRepresenter;
import org.apache.nemo.runtime.master.scheduler.ExecutorRegistry;
import org.apache.nemo.runtime.master.scheduler.PendingTaskCollectionPointer;
import org.apache.nemo.runtime.master.scheduler.TaskDispatcher;
import org.nustaq.serialization.FSTConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import static org.apache.nemo.common.TaskLoc.SF;
import static org.apache.nemo.common.TaskLoc.VM;
import static org.apache.nemo.common.TaskLoc.VM_SCALING;
import static org.apache.nemo.offloading.common.Constants.VM_WORKER_PORT;

public final class JobScaler {

  enum ExecutionStatus {
    SF_SCALE_OUT,
    VM_SCALE_OUT,
    NORMAL
  }

  private static final Logger LOG = LoggerFactory.getLogger(JobScaler.class.getName());

  private final TaskScheduledMapMaster taskScheduledMap;

  private Map<ExecutorRepresenter, Map<String, Integer>> prevScalingCountMap;

  private final AtomicBoolean isScaling = new AtomicBoolean(false);

  private final TaskLocationMap taskLocationMap;
  private final ExecutorService executorService;

  private final AtomicInteger scalingExecutorCnt = new AtomicInteger(0);

  private final Map<ExecutorRepresenter, List<ControlMessage.TaskStatInfo>> executorTaskStatMap;
  private final Map<String, ControlMessage.TaskStatInfo> taskStatInfoMap = new ConcurrentHashMap<>();

  // pair left: vm cpu, right: sf cpu
  private final Map<String, Pair<Double, Double>> executorCpuUseMap;

  private final AtomicBoolean isScalingIn = new AtomicBoolean(false);
  private final AtomicInteger isScalingInCnt = new AtomicInteger(0);

  private double prevDivide;

  private final ScheduledExecutorService scheduler;

  private final List<Integer> inputRates = new LinkedList<>();

  private final List<CompletableFuture<VMScalingWorker>> vmScalingWorkers = new ArrayList<>();

  private final int WINDOW_SIZE = 5;

  //private final List<Integer> stage0InputRates = new LinkedList<>();

  private int skipCnt = 0;

  // scale out
  private int consecutive = 0;
  // scaling in
  private int scalingInConsecutive = 0;

  private long scalingDoneTime = 0;

  private ExecutionStatus executionStatus = ExecutionStatus.NORMAL;

  private int scalingThp;
  private int baseThp = 0;

  private final TaskOffloadingManager taskOffloadingManager;

  private final EvalConf evalConf;

  private final VMWorkerManagerInMaster vmWorkerManagerInMaster;

  private long vmScalingTime = System.currentTimeMillis();

  private final PendingTaskCollectionPointer pendingTaskCollectionPointer;

  private final RuntimeMaster runtimeMaster;

  private final TaskDispatcher taskDispatcher;

  private final PlanStateManager planStateManager;

  private final ExecutorRegistry executorRegistry;

  @Inject
  private JobScaler(final TaskScheduledMapMaster taskScheduledMap,
                    final MessageEnvironment messageEnvironment,
                    final TaskLocationMap taskLocationMap,
                    final TaskOffloadingManager taskOffloadingManager,
                    final VMWorkerManagerInMaster vmWorkerManagerInMaster,
                    final EvalConf evalConf,
                    final RuntimeMaster runtimeMaster,
                    final PendingTaskCollectionPointer pendingTaskCollectionPointer,
                    final ExecutorRegistry executorRegistry,
                    final TaskDispatcher taskDispatcher,
                    final PlanStateManager planStateManager,
                    final ExecutorCpuUseMap m) {
    messageEnvironment.setupListener(MessageEnvironment.SCALE_DECISION_MESSAGE_LISTENER_ID,
      new ScaleDecisionMessageReceiver());
    this.taskScheduledMap = taskScheduledMap;
    this.prevScalingCountMap = new HashMap<>();
    this.executorTaskStatMap = new HashMap<>();
    this.executorCpuUseMap = m.getExecutorCpuUseMap();
    this.taskLocationMap = taskLocationMap;
    this.executorService = Executors.newCachedThreadPool();
    this.pendingTaskCollectionPointer = pendingTaskCollectionPointer;
    this.vmWorkerManagerInMaster = vmWorkerManagerInMaster;
    this.runtimeMaster = runtimeMaster;
    this.taskDispatcher = taskDispatcher;
    this.planStateManager = planStateManager;
    this.executorRegistry = executorRegistry;

    this.evalConf = evalConf;
    this.taskOffloadingManager = taskOffloadingManager;

    this.scheduler = Executors.newSingleThreadScheduledExecutor();
    this.scheduler.scheduleAtFixedRate(() -> {
      try {
        // TODO: avg keys, input element, output element per stage

        // triple: total stat, vm stat, sf stat
        final Map<String, Triple<Stat, Stat, Stat>> stageStat = new HashMap<>();

        for (final List<ControlMessage.TaskStatInfo> l : executorTaskStatMap.values()) {
          for (final ControlMessage.TaskStatInfo info : l) {
            final String taskId = info.getTaskId();
            final String stageId = RuntimeIdManager.getStageIdFromTaskId(taskId);
            stageStat.putIfAbsent(stageId, Triple.of(new Stat(), new Stat(), new Stat()));
            final Triple<Stat, Stat, Stat> triple = stageStat.get(stageId);

            final Stat totalStat = triple.getLeft();
            final Stat vmStat = triple.getMiddle();
            final Stat sfStat = triple.getRight();

            totalStat.numKeys += info.getNumKeys();
            totalStat.computation += info.getComputation();
            totalStat.input += info.getInputElements();
            totalStat.output += info.getOutputElements();

            if (taskLocationMap.locationMap.get(taskId).equals(SF)) {
              sfStat.numKeys += info.getNumKeys();
              sfStat.computation += info.getComputation();
              sfStat.input += info.getInputElements();
              sfStat.output += info.getOutputElements();
            } else {
              vmStat.numKeys += info.getNumKeys();
              vmStat.computation += info.getComputation();
              vmStat.input += info.getInputElements();
              vmStat.output += info.getOutputElements();
            }

            //LOG.info("Task {}, Stat {}", taskId, stat);
          }
        }

        final StringBuilder sb = new StringBuilder();

        for (final Map.Entry<String, Triple<Stat, Stat, Stat>> entry : stageStat.entrySet()) {
          final Triple<Stat, Stat, Stat> triple = entry.getValue();
          final Stat totalStat = triple.getLeft();
          final Stat vmStat = triple.getMiddle();
          final Stat sfStat = triple.getRight();

          sb.append("\nTotal stage stat:");
          sb.append("[");
          sb.append(entry.getKey());
          sb.append(",");
          sb.append(totalStat);
          sb.append("]");

          sb.append("\nVM stage stat:");
          sb.append("[");
          sb.append(entry.getKey());
          sb.append(",");
          sb.append(vmStat);
          sb.append("]");

          sb.append("\nSF stage stat:");
          sb.append("[");
          sb.append(entry.getKey());
          sb.append(",");
          sb.append(sfStat);
          sb.append("]");
        }

        LOG.info(sb.toString());

        final double cpuAvg = executorCpuUseMap.values().stream()
          .map(pair -> pair.left())
          .reduce(0.0, (x, y) -> x + y) / executorCpuUseMap.size();

        // 60초 이후에 scaling
        // LOG.info("skpCnt: {}, inputRates {}, basethp {}", skipCnt, inputRates.size(), baseThp);
        final int recentInputRate = inputRates.stream().reduce(0, (x, y) -> x + y) / WINDOW_SIZE;
        final int stage0InputRate = stageStat.get("Stage0") == null ?
          0 : (int) stageStat.get("Stage0").getLeft().input;

        final int throughput = stage0InputRate;

        final double cpuSfPlusAvg = executorCpuUseMap.values().stream()
          .map(pair -> pair.left() + pair.right())
          .reduce(0.0, (x, y) -> x + y) / executorCpuUseMap.size();

        // LOG.info("Recent input rate: {}, throughput: {}, cpuAvg: {}, cpuSfAvg: {} executorCpuUseMap: {}",
        //  recentInputRate, throughput, cpuAvg, cpuSfPlusAvg, executorCpuUseMap);


        if (cpuAvg > ScalingPolicyParameters.CPU_HIGH_THRESHOLD &&
          recentInputRate * 0.8 <= throughput ) {
          // Throttling
          executorTaskStatMap.keySet().forEach(representer -> {
              executorService.execute(() -> {
                representer.sendControlMessage(
                  ControlMessage.Message.newBuilder()
                    .setId(RuntimeIdManager.generateMessageId())
                    .setListenerId(MessageEnvironment.SCALE_DECISION_MESSAGE_LISTENER_ID)
                    .setType(ControlMessage.MessageType.Throttling)
                    .build());
              });
            }
          );
        }

        // scaling 중이면 이거 하면 안됨..!
        // scaling 하고도 slack time 가지기
        final int slackTimes = evalConf.offloadingType.equals("vm") ? 2 : 1;
        if (!isScaling.get() && !isScalingIn.get()) {

          if (stageStat.get("Stage0") != null) {
            //stage0InputRates.add(stage0InputRate);

            //if (stage0InputRates.size() > WINDOW_SIZE) {
            //  stage0InputRates.remove(0);
            //}


            if (recentInputRate > 100) {
              skipCnt += 1;
            }

            if (skipCnt > 35) {
              if (inputRates.size() == WINDOW_SIZE) {
                //final int throughput = stage0InputRates.stream().reduce(0, (x, y) -> x + y) / WINDOW_SIZE;

                if (evalConf.autoscaling) {
                  if (cpuAvg > ScalingPolicyParameters.CPU_HIGH_THRESHOLD &&
                    executionStatus == ExecutionStatus.NORMAL &&
                    System.currentTimeMillis() - scalingDoneTime >=
                    TimeUnit.SECONDS.toMillis(ScalingPolicyParameters.SLACK_TIME * slackTimes) &&
                    recentInputRate * 0.8 > throughput) {
                    // Scaling out
                    consecutive += 1;

                    if (consecutive > ScalingPolicyParameters.CONSECUTIVE) {
                      //final double burstiness = (recentInputRate / (double) (throughput * evalConf.scalingAlpha));
                      // 그다음에 task selection
                      //LOG.info("Scaling out !! Burstiness: {}", burstiness);
                      LOG.info("Scaling out !!");

                      // TODO: scaling!!
                      scalingThp = throughput;

                      //scalingOutBasedOnKeys(burstiness);
                      if (evalConf.offloadingType.equals("vm")) {
                        scalingOutToVms(throughput, recentInputRate);
                      } else {
                        scalingOutConsideringKeyAndComm(throughput, recentInputRate);
                      }

                      consecutive = 0;
                      isScaling.set(true);

                      executionStatus = ExecutionStatus.SF_SCALE_OUT;
                    }

                    LOG.info("Consecutive {}", consecutive);
                  } else if (evalConf.sfToVm &&
                    executionStatus == ExecutionStatus.SF_SCALE_OUT &&
                    cpuSfPlusAvg < ScalingPolicyParameters.CPU_HIGH_THRESHOLD &&
                    isVmScalingWorkerReady() &&
                    recentInputRate * 1.1 >= throughput) {
                    // SF to VM scaling out
                    executionStatus = ExecutionStatus.VM_SCALE_OUT;
                    moveToVmScalingWorkers();
                    vmScalingTime = System.currentTimeMillis();

                  } else if (evalConf.sfToVm &&
                    executionStatus == ExecutionStatus.VM_SCALE_OUT &&
                    cpuSfPlusAvg < ScalingPolicyParameters.CPU_LOW_THRESHOLD &&
                    recentInputRate * 1.1 >= throughput &&
                    System.currentTimeMillis() - vmScalingTime >= 30000) {

                    // VM scaling -> VM scaling in
                    scalingInConsecutive += 1;
                    if (scalingInConsecutive > ScalingPolicyParameters.CONSECUTIVE + 2) {
                      executionStatus = ExecutionStatus.NORMAL;
                      //TODO: more sophisticaed algorihtm
                      // Scaling in ...
                      LOG.info("Scaling in from VM scaling worker !!! cpu {}, input rate {}, scalingThp: {}", cpuAvg, baseThp, scalingThp);
                      scalingIn();
                      isScalingIn.set(true);
                      scalingInConsecutive = 0;
                    }

                  } else if (cpuAvg < ScalingPolicyParameters.CPU_LOW_THRESHOLD
                    && System.currentTimeMillis() - scalingDoneTime >=
                    TimeUnit.SECONDS.toMillis(ScalingPolicyParameters.SLACK_TIME * slackTimes)
                    && executionStatus == ExecutionStatus.SF_SCALE_OUT && recentInputRate * 1.1 >= throughput) {

                    // SF scaling in

                    scalingInConsecutive += 1;

                    if (scalingInConsecutive > ScalingPolicyParameters.CONSECUTIVE) {
                      executionStatus = ExecutionStatus.NORMAL;
                      //TODO: more sophisticaed algorihtm
                      // Scaling in ...
                      LOG.info("Scaling in !!! cpu {}, input rate {}, scalingThp: {}", cpuAvg, baseThp, scalingThp);
                      scalingIn();
                      isScalingIn.set(true);
                      scalingInConsecutive = 0;
                    }

                  } else {
                    consecutive = 0;
                    scalingInConsecutive = 0;
                  }
                }
              }
            }
          }
        }

      } catch (final Exception e) {
        e.printStackTrace();
        throw new RuntimeException(e);
      }

    }, 1, 1, TimeUnit.SECONDS);
  }

  private boolean isVmScalingWorkerReady() {
    boolean ready = true;
    for (final CompletableFuture<VMScalingWorker> w : vmScalingWorkers) {
      if (!w.isDone()) {
        ready = false;
        break;
      }
    }

    LOG.info("isVmScalingWorkerReady {}", ready);
    return ready;
  }

  public void broadcastInfo(final ControlMessage.ScalingMessage msg) {

    if (msg.getInfo().startsWith("INPUT")) {
      // input rate
      final Integer inputRate = Integer.valueOf(msg.getInfo().split("INPUT ")[1]);
      //LOG.info("Input rate {}", inputRate);

      inputRates.add(inputRate);
      if (inputRates.size() > WINDOW_SIZE) {
        inputRates.remove(0);
      }

    } else {
      LOG.info("Broadcast info {}", msg.getInfo());
      for (final ExecutorRepresenter executorRepresenter : taskScheduledMap.getScheduledStageTasks().keySet()) {
        executorRepresenter.sendControlMessage(ControlMessage.Message.newBuilder()
          .setId(RuntimeIdManager.generateMessageId())
          .setListenerId(MessageEnvironment.SCALE_DECISION_MESSAGE_LISTENER_ID)
          .setType(ControlMessage.MessageType.BroadcastInfo)
          .setBroadcastInfoMsg(ControlMessage.BroadcastInfoMessage.newBuilder()
            .setInfo(msg.getInfo())
            .build())
          .build());
      }
    }
  }

  public void proactive(final ControlMessage.ScalingMessage msg) {
    scalingIn();
    //TODO: waiting scaling in
    LOG.info("Waiting isScalingIn");
    while (isScalingIn.get()) {
      try {
        Thread.sleep(500);
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
    }
    LOG.info("Waiting done isScalingIn");
    scalingOutBasedOnKeys(prevDivide);
  }

  public void scalingOut(final ControlMessage.ScalingMessage msg) {

    final double divide = msg.getDivide();
    final int prevScalingCount = sumCount();

    prevDivide = divide;

    if (prevScalingCount > 0) {
      throw new RuntimeException("Scaling count should be equal to 0 when scaling out... but "
        + prevScalingCount + ", " + prevScalingCountMap);
    }

    if (!isScaling.compareAndSet(false, true)) {
      throw new RuntimeException("Scaling false..." + isScaling.get());
    }

    final int query = msg.hasQuery() ? msg.getQuery() : 0;
    final List<Double> ratioList = msg.getStageRatioList();
    if (msg.getDecision().equals("op")) {
      scalingOutBasedOnKeys(divide);
    } else {
      scalingOutToWorkers(divide, ratioList);
    }
  }

  private ControlMessage.RequestScalingMessage buildRequestScalingMessage(
    final Map<String, List<String>> offloadTaskMap,
    final List<ControlMessage.TaskLocation> locations,
    final boolean isScaleOut,
    final boolean mvToVmScaling,
    final Map<String, String> newExecutorAddrMap) {

    final ControlMessage.RequestScalingMessage.Builder builder =
    ControlMessage.RequestScalingMessage.newBuilder();

    builder.setRequestId(RuntimeIdManager.generateMessageId());
    builder.setIsScaleOut(isScaleOut);
    builder.addAllTaskLocation(locations);
    builder.setMoveToVmScaling(mvToVmScaling);

    for (final Map.Entry<String, List<String>> entry : offloadTaskMap.entrySet()) {
      builder.addEntry(ControlMessage.RequestScalingEntryMessage
        .newBuilder()
        .setStageId(entry.getKey())
        .addAllOffloadTasks(entry.getValue())
        .build());
    }

    for (final Map.Entry<String, String> entry : taskScheduledMap.getTaskExecutorIdMap().entrySet()) {
      builder.addTaskExecutorId(ControlMessage.TaskExecutorIdEntryMessage
        .newBuilder()
        .setTaskId(entry.getKey())
        .setExecutorId(entry.getValue())
        .build());
    }

    for (final Map.Entry<String, String> entry : newExecutorAddrMap.entrySet()) {
        builder.addExecutorAddrInfos(ControlMessage.LocalExecutorAddressInfoMessage
          .newBuilder()
          .setExecutorId(entry.getKey())
          .setAddress(entry.getValue())
          .setPort(VM_WORKER_PORT)
          .build());
    }

    return builder.build();
  }


  private int getLocInt(final TaskLoc loc) {
    if (loc == VM) {
      return 0;
    } else if (loc == SF) {
      return 1;
    } else if (loc == VM_SCALING) {
      return 2;
    } else {
      throw new RuntimeException("Invalid location " + loc);
    }
  }

  private List<ControlMessage.TaskLocation> encodeTaskLocationMap() {
    final List<ControlMessage.TaskLocation> list = new ArrayList<>(taskLocationMap.locationMap.size());
    for (final Map.Entry<String, TaskLoc> entry : taskLocationMap.locationMap.entrySet()) {
      list.add(ControlMessage.TaskLocation.newBuilder()
        .setTaskId(entry.getKey())
        .setLocation(getLocInt(entry.getValue()))
        .build());
    }
    return list;
  }

  private List<ControlMessage.TaskStatInfo> shuffle(final List<ControlMessage.TaskStatInfo> taskStatInfos) {
    // sort by keys
    final List<ControlMessage.TaskStatInfo> copyInfos = new ArrayList<>(taskStatInfos);

    Collections.shuffle(copyInfos);
    return copyInfos;
  }

  private List<ControlMessage.TaskStatInfo> sortByKeys(final List<ControlMessage.TaskStatInfo> taskStatInfos) {
    // sort by keys
    final List<ControlMessage.TaskStatInfo> copyInfos = new ArrayList<>(taskStatInfos);

    Collections.sort(copyInfos, new Comparator<ControlMessage.TaskStatInfo>() {
      @Override
      public int compare(ControlMessage.TaskStatInfo o1, ControlMessage.TaskStatInfo o2) {
        final int cmp = Integer.compare(o1.getNumKeys(), o2.getNumKeys());
        if (cmp == 0) {
          return o1.getTaskId().compareTo(o2.getTaskId());
        } else {
          return cmp;
        }
      }
    });

    return copyInfos;
  }

  private long getRelayOverhead(final ControlMessage.TaskStatInfo taskStatInfo) {
    final List<String> inputTasks = taskOffloadingManager.getTaskInputTasksMap()
      .getOrDefault(taskStatInfo.getTaskId(), Collections.emptyList());
    final List<String> outputTasks = taskOffloadingManager.getTaskOutputTasksMap()
      .getOrDefault(taskStatInfo.getTaskId(), Collections.emptyList());

    final double alpha = ScalingPolicyParameters.RELAY_OVERHEAD;

    long relayEvents = 0;

    /*
    for (final String inputTask : inputTasks) {
      if (taskLocationMap.locationMap.get(inputTask).equals(SF)) {
        relayEvents += taskStatInfoMap.get(inputTask).getOutputElements();
      }
    }
    */

    LOG.info("Output task of {}: {}", taskStatInfo.getTaskId(), outputTasks);

    /*

    for (final String outputTask : outputTasks) {
       if (taskLocationMap.locationMap.get(outputTask).equals(SF)) {
        relayEvents += taskStatInfo.getOutputElements();
      }
    }
    */

    relayEvents += taskStatInfo.getInputElements();
    relayEvents += taskStatInfo.getOutputElements();

    //TODO: add overhead
    final long overhead = (long) (alpha * relayEvents);
    return overhead;
  }

  private void moveToVmScalingWorkers() {

    final int numWorkers = vmScalingWorkers.size();
    LOG.info("Moving task to {} vm workers", numWorkers);

    final Map<String, String> newExecutorAddrMap = new HashMap<>();
    final List<VMScalingWorker> workers = vmScalingWorkers.stream().map(future -> {
      try {
        final VMScalingWorker w = future.get();
        newExecutorAddrMap.put(w.getExecutorId(), w.getVmAddress());
        return w;
      } catch (final Exception e) {
        e.printStackTrace();
        throw new RuntimeException(e);
      }
    }).collect(Collectors.toList());

    final Map<String, List<String>> unloadTaskMap = new HashMap<>();

    final List<String> mvTasks = new LinkedList<>();

    for (final String key : taskLocationMap.locationMap.keySet()) {
      if (taskLocationMap.locationMap.get(key) == SF) {
        final String stageId = RuntimeIdManager.getStageIdFromTaskId(key);
        unloadTaskMap.putIfAbsent(stageId, new ArrayList<>());
        final List<String> unloadTasks = unloadTaskMap.get(stageId);
        unloadTasks.add(key);
        taskLocationMap.locationMap.put(key, VM_SCALING);
        mvTasks.add(key);
      }
    }


    // Task to VM worker mapping
    taskScheduledMap.keepOnceCurrentTaskExecutorIdMap();

    int cnt = 0;
    final int tasksPerWorker = mvTasks.size() / workers.size();
    for (final String key : taskLocationMap.locationMap.keySet()) {
      if (taskLocationMap.locationMap.get(key) == VM_SCALING) {
        final int vmWorkerIndex = Math.min(cnt / tasksPerWorker, workers.size() - 1);
        final String executorId = workers.get(vmWorkerIndex).getExecutorId();
        taskScheduledMap.getTaskExecutorIdMap().put(key, executorId);
        LOG.info("Moving Task {} to {}", key, executorId);
        cnt += 1;
      }
    }

    // Send 1) taskExecutorIdMap and 2) location map to VM scaling workers
    final Map<String, TaskLoc> locationMap = taskLocationMap.locationMap;
    final Map<String, String> taskExecutorIdMap = taskScheduledMap.getTaskExecutorIdMap();
    final ByteBuf byteBuf = ByteBufAllocator.DEFAULT.buffer();
    final ByteBufOutputStream bos = new ByteBufOutputStream(byteBuf);

    final FSTConfiguration conf = FSTSingleton.getInstance();
    try {
      conf.encodeToStream(bos, locationMap);
      conf.encodeToStream(bos, taskExecutorIdMap);
      bos.close();
    } catch (final Exception e) {
      e.printStackTrace();
      throw new RuntimeException(e);
    }

    for (final VMScalingWorker worker : workers) {
      final ByteBuf b = byteBuf.retainedDuplicate();
      worker.send(new OffloadingMasterEvent(OffloadingMasterEvent.Type.EXECUTOR_INIT_INFO, b));
    }

    try {
      Thread.sleep(3000);
    } catch (InterruptedException e) {
      e.printStackTrace();
    }

    // Send to vm workers
    final List<ControlMessage.TaskLocation> locations = encodeTaskLocationMap();
    for (final ExecutorRepresenter representer :
      taskScheduledMap.getScheduledStageTasks().keySet()) {
      executorService.execute(() -> {
        representer.sendControlMessage(
          ControlMessage.Message.newBuilder()
            .setId(RuntimeIdManager.generateMessageId())
            .setListenerId(MessageEnvironment.SCALE_DECISION_MESSAGE_LISTENER_ID)
            .setType(ControlMessage.MessageType.RequestScaling)
            .setRequestScalingMsg(
              buildRequestScalingMessage(
                unloadTaskMap, locations, false, true, newExecutorAddrMap))
            .build());
      });
    }
  }

  private void scalingOutToVms(final long thp, final long input_rate) {
    final Map<ExecutorRepresenter, Map<String, List<String>>> workerOffloadTaskMap = new HashMap<>();

    int offloadingCnt = 0;

    final double ratio = (1 - ((double)thp * evalConf.scalingAlpha / input_rate));

    // # of vm scaling workers for each vm
    final int numWorkers = (int) Math.ceil((1 / ratio));
    final Map<String, String> taskExecutorIdMap = taskScheduledMap.getTaskExecutorIdMap();

    LOG.info("ratio {}, # of vm scaling workers: {}", ratio, numWorkers);

    for (final ExecutorRepresenter representer :
      taskScheduledMap.getScheduledStageTasks().keySet()) {

      workerOffloadTaskMap.put(representer, new HashMap<>());

      final Map<String, List<String>> offloadTaskMap = workerOffloadTaskMap.get(representer);

      final List<ControlMessage.TaskStatInfo> taskStatInfos = executorTaskStatMap.get(representer);
      final long totalComputation = taskStatInfos.stream().map(info -> info.getComputation())
        .reduce(0L, (x,y) -> x + y);

      LOG.info("Task stats of executor {}: {}", representer.getExecutorId(), taskStatInfos);

      final long totalOffloadComputation = (long) (totalComputation * ratio);

      LOG.info("Offloading ratio: {}, alpha: {}, input_rate: {}, thp: {}", ratio, evalConf.scalingAlpha, input_rate, thp);

      final List<ControlMessage.TaskStatInfo> copyInfos;
      copyInfos = shuffle(taskStatInfos);

      LOG.info("Total comp: {}, offload comp: {}", totalComputation, totalOffloadComputation);

      int i = 0;
      long offloadComputation = 0;
      final long compForEachVmScalingWorker = totalOffloadComputation / numWorkers;
      while (offloadComputation < totalOffloadComputation && i < copyInfos.size()) {

        final int executorIndex = Math.min((int) (offloadComputation / compForEachVmScalingWorker),
          numWorkers - 1);

        final String newExecutorId = representer.getExecutorId() + "-" + executorIndex;

        final ControlMessage.TaskStatInfo taskStatInfo = copyInfos.get(i);
        final String stageId = RuntimeIdManager.getStageIdFromTaskId(taskStatInfo.getTaskId());
        offloadTaskMap.putIfAbsent(stageId, new ArrayList<>());
        final List<String> offloadTask = offloadTaskMap.get(stageId);

        final TaskLoc loc = taskLocationMap.locationMap.get(taskStatInfo.getTaskId());
        LOG.info("Offloading {}, key {}, comp {}, output {}, Executor {}",
          taskStatInfo.getTaskId(),
          taskStatInfo.getNumKeys(),
          taskStatInfo.getComputation(),
          taskStatInfo.getOutputElements(),
          newExecutorId);

        if (loc == VM) {
          long overhead = 0;

          if (overhead < taskStatInfo.getComputation()) {
            offloadingCnt += 1;
            offloadTask.add(taskStatInfo.getTaskId());
            taskLocationMap.locationMap.put(taskStatInfo.getTaskId(), VM_SCALING);
            offloadComputation += taskStatInfo.getComputation();

            taskExecutorIdMap.put(taskStatInfo.getTaskId(), newExecutorId);

            LOG.info("Task offloading overhead {}, computation {}, offloadComp {}",
              overhead, taskStatInfo.getComputation(), offloadComputation);
          }
        }

        i += 1;
      }
    }

    LOG.info("Total offloading count: {}", offloadingCnt);

    LOG.info("TaskExecutorIdMap: {}", taskExecutorIdMap);

    final List<ControlMessage.TaskLocation> taskLocations = encodeTaskLocationMap();

    int i = 0;
    for (final Map.Entry<ExecutorRepresenter,
      Map<String, List<String>>> entry : workerOffloadTaskMap.entrySet()) {
      scalingExecutorCnt.getAndIncrement();

      final ExecutorRepresenter representer = entry.getKey();
      final Map<String, List<String>> offloadTaskMap = entry.getValue();

      // scaling out message
      LOG.info("Send scaling out message {} to {}", entry.getValue(),
        representer.getExecutorId());

      i += 1;

      executorService.execute(() -> {
        representer.sendControlMessage(
          ControlMessage.Message.newBuilder()
            .setId(RuntimeIdManager.generateMessageId())
            .setListenerId(MessageEnvironment.SCALE_DECISION_MESSAGE_LISTENER_ID)
            .setType(ControlMessage.MessageType.RequestScaling)
            .setRequestScalingMsg(buildRequestScalingMessage(offloadTaskMap, taskLocations, true, false, new HashMap<>()))
            .build());
      });
    }
  }

  private void createVmWorkers() {

  }

  private void scalingOutConsideringKeyAndComm(final long thp, final long input_rate) {

    final double ratio = (1 - (thp * evalConf.scalingAlpha) / input_rate);
    // # of vm scaling workers for each vm
    final int numWorkers = (int) Math.ceil((1 / ratio));

    // create vm workers
    if (evalConf.sfToVm) {
      vmScalingWorkers.addAll(vmWorkerManagerInMaster.createWorkers(numWorkers));
      LOG.info("ratio {}, # of vm scaling workers: {}", ratio, numWorkers);
    }

    final Map<ExecutorRepresenter, Map<String, List<String>>> workerOffloadTaskMap = new HashMap<>();

    int offloadingCnt = 0;

    for (final ExecutorRepresenter representer :
      taskScheduledMap.getScheduledStageTasks().keySet()) {

      workerOffloadTaskMap.put(representer, new HashMap<>());

      final Map<String, List<String>> offloadTaskMap = workerOffloadTaskMap.get(representer);

      final List<ControlMessage.TaskStatInfo> taskStatInfos = executorTaskStatMap.get(representer);
      final long totalComputation = taskStatInfos.stream().map(info -> info.getComputation())
        .reduce(0L, (x,y) -> x + y);

      LOG.info("Task stats of executor {}: {}", representer.getExecutorId(), taskStatInfos);

      final long totalOffloadComputation = (long) (totalComputation * ratio);

      LOG.info("Offloading ratio: {}, alpha: {}, input_rate: {}, thp: {}", ratio, evalConf.scalingAlpha, input_rate, thp);

      final List<ControlMessage.TaskStatInfo> copyInfos;
      if (evalConf.offloadingType.equals("vm") || evalConf.randomSelection) {
        copyInfos = shuffle(taskStatInfos);
      } else {
        copyInfos = sortByKeys(taskStatInfos);
      }

      LOG.info("Total comp: {}, offload comp: {}", totalComputation, totalOffloadComputation);

      int i = 0;
      long offloadComputation = 0;
      while (offloadComputation < totalOffloadComputation && i < copyInfos.size()) {

        final ControlMessage.TaskStatInfo taskStatInfo = copyInfos.get(i);
        final String stageId = RuntimeIdManager.getStageIdFromTaskId(taskStatInfo.getTaskId());
        offloadTaskMap.putIfAbsent(stageId, new ArrayList<>());
        final List<String> offloadTask = offloadTaskMap.get(stageId);

        final TaskLoc loc = taskLocationMap.locationMap.get(taskStatInfo.getTaskId());
        LOG.info("Offloading {}, key {}, comp {}, output {}, Executor {}",
          taskStatInfo.getTaskId(),
          taskStatInfo.getNumKeys(),
          taskStatInfo.getComputation(),
          taskStatInfo.getOutputElements(),
          representer.getExecutorId());

        if (loc == VM) {
          long overhead;
          if (evalConf.randomSelection) {
            overhead = 0;
          } else {
            overhead = getRelayOverhead(taskStatInfo);
          }

          if (overhead < taskStatInfo.getComputation()) {
            offloadingCnt += 1;
            offloadTask.add(taskStatInfo.getTaskId());
            taskLocationMap.locationMap.put(taskStatInfo.getTaskId(), SF);
            offloadComputation += taskStatInfo.getComputation();
            // TODO: check relay overhead
            offloadComputation -= overhead;

            LOG.info("Task offloading overhead {}, computation {}, offloadComp {}",
              overhead, taskStatInfo.getComputation(), offloadComputation);
          }
        }

        i += 1;
      }
    }


    LOG.info("Total offloading count: {}", offloadingCnt);

    final List<ControlMessage.TaskLocation> taskLocations = encodeTaskLocationMap();


    int i = 0;

    for (final Map.Entry<ExecutorRepresenter,
      Map<String, List<String>>> entry : workerOffloadTaskMap.entrySet()) {
      scalingExecutorCnt.getAndIncrement();

      final ExecutorRepresenter representer = entry.getKey();
      final Map<String, List<String>> offloadTaskMap = entry.getValue();

      // scaling out message
      LOG.info("Send scaling out message {} to {}", entry.getValue(),
        representer.getExecutorId());

      i += 1;

      executorService.execute(() -> {
        representer.sendControlMessage(
          ControlMessage.Message.newBuilder()
            .setId(RuntimeIdManager.generateMessageId())
            .setListenerId(MessageEnvironment.SCALE_DECISION_MESSAGE_LISTENER_ID)
            .setType(ControlMessage.MessageType.RequestScaling)
            .setRequestScalingMsg(buildRequestScalingMessage(offloadTaskMap, taskLocations, true, false, new HashMap<>()))
            .build());
      });
    }
  }

  private void scalingOutBasedOnKeys(final double divide) {
    final Map<ExecutorRepresenter, Map<String, List<String>>> workerOffloadTaskMap = new HashMap<>();

    for (final ExecutorRepresenter representer :
      taskScheduledMap.getScheduledStageTasks().keySet()) {

      workerOffloadTaskMap.put(representer, new HashMap<>());

      final Map<String, List<String>> offloadTaskMap = workerOffloadTaskMap.get(representer);

      final List<ControlMessage.TaskStatInfo> taskStatInfos = executorTaskStatMap.get(representer);

      // sort by keys
      final List<ControlMessage.TaskStatInfo> copyInfos = sortByKeys(taskStatInfos);

      final int countToOffload = (taskStatInfos.size() - (int) (taskStatInfos.size() / divide));

      int offloadedCnt = 0;

      LOG.info("Count to offload: {}", countToOffload);
      LOG.info("copInfos {}", copyInfos);

      for (final ControlMessage.TaskStatInfo taskStatInfo : copyInfos) {
        final String stageId = RuntimeIdManager.getStageIdFromTaskId(taskStatInfo.getTaskId());
        offloadTaskMap.putIfAbsent(stageId, new ArrayList<>());
        final List<String> offloadTask = offloadTaskMap.get(stageId);

        if (offloadedCnt < countToOffload) {
          final TaskLoc loc = taskLocationMap.locationMap.get(taskStatInfo.getTaskId());
          LOG.info("Offloading {}, key {}, Executor {}", taskStatInfo.getTaskId(), taskStatInfo.getNumKeys(), representer.getExecutorId());

          if (loc == VM) {
            offloadTask.add(taskStatInfo.getTaskId());
            taskLocationMap.locationMap.put(taskStatInfo.getTaskId(), SF);
            offloadedCnt += 1;
          }
        }
      }
    }

    final List<ControlMessage.TaskLocation> taskLocations = encodeTaskLocationMap();

    for (final Map.Entry<ExecutorRepresenter,
      Map<String, List<String>>> entry : workerOffloadTaskMap.entrySet()) {
      scalingExecutorCnt.getAndIncrement();

      final ExecutorRepresenter representer = entry.getKey();
      final Map<String, List<String>> offloadTaskMap = entry.getValue();

      // scaling out message
      LOG.info("Send scaling out message {} to {}", entry.getValue(),
        representer.getExecutorId());

      executorService.execute(() -> {
        representer.sendControlMessage(
          ControlMessage.Message.newBuilder()
            .setId(RuntimeIdManager.generateMessageId())
            .setListenerId(MessageEnvironment.SCALE_DECISION_MESSAGE_LISTENER_ID)
            .setType(ControlMessage.MessageType.RequestScaling)
            .setRequestScalingMsg(buildRequestScalingMessage(offloadTaskMap, taskLocations, true, false, new HashMap<>()))
            .build());
      });
    }
  }

  private void scalingOutToWorkers(final double divide,
                                   final List<Double> offloadingRatio) {

    // 1. update all task location
    final Map<ExecutorRepresenter, Map<String, List<String>>> workerOffloadTaskMap = new HashMap<>();

    for (final ExecutorRepresenter representer :
      taskScheduledMap.getScheduledStageTasks().keySet()) {

      workerOffloadTaskMap.put(representer, new HashMap<>());

      final Map<String, List<String>> tasks = taskScheduledMap.getScheduledStageTasks(representer);
      final Map<String, List<String>> offloadTaskMap = workerOffloadTaskMap.get(representer);
      final int totalTasks = tasks.values().stream().map(l -> l.size()).reduce(0, (x,y) -> x+y);

      // If offloading ratio is empty, it means that just scaling with the divide value
      if (offloadingRatio.isEmpty()) {

        for (final Map.Entry<String, List<String>> entry : tasks.entrySet()) {

          final int countToOffload = (int) (entry.getValue().size() - (entry.getValue().size() / divide));
          final List<String> offloadTask = new ArrayList<>();
          offloadTaskMap.put(entry.getKey(), offloadTask);

          int offloadedCnt = 0;
          for (final String taskId : entry.getValue()) {
            if (offloadedCnt < countToOffload) {
              final TaskLoc loc = taskLocationMap.locationMap.get(taskId);

              if (loc == VM) {
                offloadTask.add(taskId);
                taskLocationMap.locationMap.put(taskId, SF);
                offloadedCnt += 1;
              }
            }
          }
        }
      } else {
        // stage-offloading
        // we set the number for each stage
        final int totalCountToOffload = (int) (totalTasks - totalTasks / divide);
        final Map<String, Integer> stageOffloadCnt = new HashMap<>();
        for (int i = 0; i < offloadingRatio.size(); i++) {
          final String stageId = "Stage" + i;
          final int offloadCnt = (int) (totalCountToOffload * offloadingRatio.get(i));
          stageOffloadCnt.put(stageId, offloadCnt);
        }

        LOG.info("Offloading ratio: {}, stageOffloadCnt: {}", offloadingRatio, stageOffloadCnt);

        for (final Map.Entry<String, List<String>> entry : tasks.entrySet()) {

          //LOG.info("Key {}", entry.getKey());
          final int countToOffload = stageOffloadCnt.getOrDefault(entry.getKey(), 0);
          final List<String> offloadTask = new ArrayList<>();
          offloadTaskMap.put(entry.getKey(), offloadTask);

          int offloadedCnt = 0;
          for (final String taskId : entry.getValue()) {
            if (offloadedCnt < countToOffload) {
              final TaskLoc loc = taskLocationMap.locationMap.get(taskId);

              if (loc == VM) {
                LOG.info("Offloading {} ", taskId);
                offloadTask.add(taskId);
                taskLocationMap.locationMap.put(taskId, SF);
                offloadedCnt += 1;
              }
            }
          }
        }
      }
    }

    final List<ControlMessage.TaskLocation> taskLocations = encodeTaskLocationMap();

    for (final Map.Entry<ExecutorRepresenter,
      Map<String, List<String>>> entry : workerOffloadTaskMap.entrySet()) {
      scalingExecutorCnt.getAndIncrement();

      final ExecutorRepresenter representer = entry.getKey();
      final Map<String, List<String>> offloadTaskMap = entry.getValue();

      // scaling out message
      LOG.info("Send scaling out message {} to {}", entry.getValue(),
        representer.getExecutorId());

      executorService.execute(() -> {
        representer.sendControlMessage(
          ControlMessage.Message.newBuilder()
            .setId(RuntimeIdManager.generateMessageId())
            .setListenerId(MessageEnvironment.SCALE_DECISION_MESSAGE_LISTENER_ID)
            .setType(ControlMessage.MessageType.RequestScaling)
            .setRequestScalingMsg(buildRequestScalingMessage(offloadTaskMap, taskLocations, true, false, new HashMap<>()))
            .build());
      });
    }
  }

  private int sumCount() {
    return prevScalingCountMap.values().stream()
      .map(m -> m.values().stream().reduce(0, (x,y) -> x+y)) .reduce(0, (x, y) -> x+y);
  }

  public void scalingIn() {

    if (isScalingIn.get() || isScalingInCnt.get() > 0){
      throw new RuntimeException("Scaling in true..." + isScalingIn + ", " + isScalingInCnt);
    }

    isScalingIn.set(true);
    isScalingInCnt.set(taskScheduledMap.getScheduledStageTasks().keySet().size());

    final Map<String, List<String>> unloadTaskMap = new HashMap<>();

    for (final String key : taskLocationMap.locationMap.keySet()) {
      if (taskLocationMap.locationMap.get(key) == SF
        || taskLocationMap.locationMap.get(key) == VM_SCALING) {
        final String stageId = RuntimeIdManager.getStageIdFromTaskId(key);
        unloadTaskMap.putIfAbsent(stageId, new ArrayList<>());
        final List<String> unloadTasks = unloadTaskMap.get(stageId);
        unloadTasks.add(key);
        taskLocationMap.locationMap.put(key, VM);
      }
    }

    final Map<String, String> m = taskScheduledMap.getPrevTaskExecutorIdMap();
    taskScheduledMap.getTaskExecutorIdMap().putAll(m);

    // Send the task-executor id to the current vm scaling worker
    final List<VMScalingWorker> workers = vmScalingWorkers.stream().map(f -> {
      try {
        return f.get();
      } catch (Exception e) {
        e.printStackTrace();
        throw new RuntimeException(e);
      }
    }).collect(Collectors.toList());

    final Map<String, String> taskExecutorIdMap = taskScheduledMap.getTaskExecutorIdMap();
    final ByteBuf byteBuf = ByteBufAllocator.DEFAULT.buffer();
    final ByteBufOutputStream bos = new ByteBufOutputStream(byteBuf);

    final FSTConfiguration conf = FSTSingleton.getInstance();
    try {
      conf.encodeToStream(bos, taskExecutorIdMap);
      bos.close();
    } catch (final Exception e) {
      e.printStackTrace();
      throw new RuntimeException(e);
    }

    for (final VMScalingWorker worker : workers) {
      final ByteBuf b = byteBuf.retainedDuplicate();
      worker.send(new OffloadingMasterEvent(OffloadingMasterEvent.Type.EXECUTOR_FINISH_INFO, b));
    }

    final List<ControlMessage.TaskLocation> locations = encodeTaskLocationMap();

    for (final ExecutorRepresenter representer :
      taskScheduledMap.getScheduledStageTasks().keySet()) {
      executorService.execute(() -> {
        representer.sendControlMessage(
          ControlMessage.Message.newBuilder()
            .setId(RuntimeIdManager.generateMessageId())
            .setListenerId(MessageEnvironment.SCALE_DECISION_MESSAGE_LISTENER_ID)
            .setType(ControlMessage.MessageType.RequestScaling)
            .setRequestScalingMsg(buildRequestScalingMessage(unloadTaskMap, locations, false, false, new HashMap<>()))
            .build());
      });
    }
  }

  private final Set<String> prevMovedTask = new HashSet<>();

  public synchronized void sendPrevMovedTaskStopSignal(final int num,
                                                       final int stageId) {

    taskDispatcher.setReclaiming(true);

    final Map<String, String> taskExecutorIdMap = taskScheduledMap.getTaskExecutorIdMap();

    int cnt = 0;
    final Iterator<String> iterator = prevMovedTask.iterator();

    while (iterator.hasNext()) {
      final String taskId = iterator.next();
      final String executorId = taskExecutorIdMap.get(taskId);

      if (RuntimeIdManager.getStageIdFromTaskId(taskId).equals("Stage" + stageId)) {
        taskScheduledMap.stopTask(taskId);
        cnt += 1;
        iterator.remove();

        if (cnt >= num) {
          break;
        }
      }
    }
  }

  public synchronized void sendTaskStopSignal(final int num,
                                              final int stageId) {

    taskDispatcher.setReclaiming(false);

    LOG.info("Send task stop signal");
    final Map<String, String> taskExecutorIdMap = taskScheduledMap.getTaskExecutorIdMap();

    int stopped = 0;

    for (final Map.Entry<String, String> entry : taskExecutorIdMap.entrySet()) {
      final String taskId = entry.getKey();
      final String executorId = entry.getValue();

      if (!prevMovedTask.contains(taskId)) {
        if (!executorRegistry.getExecutorRepresentor(executorId)
          .getContainerType().equals(ResourcePriorityProperty.SOURCE)
          && RuntimeIdManager.getStageIdFromTaskId(taskId).equals("Stage" + stageId)) {
          LOG.info("Stop task {}", taskId);
          taskScheduledMap.stopTask(taskId);
          stopped += 1;
          prevMovedTask.add(taskId);

          if (stopped == num) {
            break;
          }
        }
      }
    }
  }

  private void sendScalingOutDoneToAllWorkers() {
    LOG.info("Send scale out done to all workers");

    final List<String> offloadedTasks = taskLocationMap.locationMap.entrySet()
      .stream().filter(entry -> entry.getValue().equals(SF))
      .map(entry -> entry.getKey())
      .collect(Collectors.toList());

    LOG.info("Offloaded tasks: {}", offloadedTasks);

    for (final ExecutorRepresenter representer : taskScheduledMap.getScheduledStageTasks().keySet()) {
      final long id = RuntimeIdManager.generateMessageId();
      executorService.execute(() -> {
      representer.sendControlMessage(ControlMessage.Message.newBuilder()
        .setId(id)
        .setListenerId(MessageEnvironment.SCALE_DECISION_MESSAGE_LISTENER_ID)
        .setType(ControlMessage.MessageType.GlobalScalingReadyDone)
        .setGlobalScalingDoneMsg(ControlMessage.GlobalScalingDoneMessage.newBuilder()
          .setRequestId(id)
          .addAllOffloadedTasks(offloadedTasks)
          .build())
        .build());
      });
    }
  }

  public final class ScaleDecisionMessageReceiver implements MessageListener<ControlMessage.Message> {
    @Override
    public void onMessage(final ControlMessage.Message message) {
      switch (message.getType()) {
        case LocalScalingReadyDone: {
          final ControlMessage.LocalScalingDoneMessage localScalingDoneMessage = message.getLocalScalingDoneMsg();
          final String executorId = localScalingDoneMessage.getExecutorId();
          final int type = localScalingDoneMessage.getType();

          switch (type) {
            case 1: {
              final ExecutorRepresenter executorRepresenter = executorRegistry.getExecutorRepresentor(executorId);
              LOG.info("Receive LocalScalingDone for {}", executorId);

              final int cnt = scalingExecutorCnt.decrementAndGet();
              if (cnt == 0) {
                scalingDoneTime = System.currentTimeMillis();
                if (isScaling.compareAndSet(true, false)) {
                  //sendScalingOutDoneToAllWorkers();
                }
              }

              break;
            }
            case 2: {
              // Scaling in done
              // this is useful for proactive migration

              LOG.info("Scaling in done signal get {}", executorId);
              if (isScalingInCnt.decrementAndGet() == 0) {
                scalingDoneTime = System.currentTimeMillis();
                isScaling.compareAndSet(true, false);
                isScalingIn.set(false);
              }

              break;
            }
          }

          break;
        }
        case TaskStatSignal: {
          final ControlMessage.TaskStatMessage taskStatMessage = message.getTaskStatMsg();
          final String executorId = taskStatMessage.getExecutorId();
          //LOG.info("Receive taskstatSignal from {}", executorId);
          final List<ControlMessage.TaskStatInfo> taskStatInfos = taskStatMessage.getTaskStatsList();
          final ExecutorRepresenter executorRepresenter = executorRegistry.getExecutorRepresentor(executorId);

          if (executorRepresenter != null) {
            executorCpuUseMap.put(executorRepresenter.getExecutorId(),
              Pair.of(taskStatMessage.getCpuUse(), taskStatMessage.getSfCpuUse()));

            executorTaskStatMap.put(executorRepresenter, taskStatInfos);

            for (final ControlMessage.TaskStatInfo taskStatInfo : taskStatInfos) {
              taskStatInfoMap.putIfAbsent(taskStatInfo.getTaskId(), taskStatInfo);
            }
          }
          break;
        }
        default:
          throw new IllegalMessageException(new Exception(message.toString()));
      }
    }

    @Override
    public void onMessageWithContext(ControlMessage.Message message, MessageContext messageContext) {
      throw new RuntimeException("Not supported");
    }
  }

  class Stat {
    public int numKeys = 0;
    public long computation = 0;
    public long input = 0;
    public long output = 0;

    @Override
    public String toString() {
      return "key: " + numKeys + ", comp: " + computation + ", input: " + input + ", output: " + output;
    }
  }
}
