package org.apache.nemo.runtime.executor;

import org.apache.commons.math3.stat.descriptive.DescriptiveStatistics;
import org.apache.nemo.common.Pair;
import org.apache.nemo.conf.EvalConf;
import org.apache.nemo.conf.JobConf;
import org.apache.nemo.runtime.common.RuntimeIdManager;
import org.apache.nemo.runtime.common.comm.ControlMessage;
import org.apache.nemo.runtime.common.message.MessageEnvironment;
import org.apache.nemo.runtime.common.message.PersistentConnectionToMasterMap;
import org.apache.nemo.runtime.executor.common.TaskExecutor;
import org.apache.reef.tang.annotations.Parameter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import java.util.*;
import java.util.concurrent.*;
import java.util.stream.Collectors;

public final class TaskOffloader {
  private static final Logger LOG = LoggerFactory.getLogger(TaskOffloader.class.getName());

  private final ScheduledExecutorService monitorThread;
  private final SystemLoadProfiler profiler;

  private final long r;
  private final int k;
  private final double threshold;
  private int currConsecutive = 0;

  private final TaskEventRateCalculator taskEventRateCalculator;
  private final CpuEventModel cpuEventModel;

  // key: offloaded task executor, value: start time of offloading
  private final List<Pair<TaskExecutor, Long>> offloadedExecutors;
  private final ConcurrentMap<TaskExecutor, Boolean> taskExecutorMap;
  private long prevDecisionTime = System.currentTimeMillis();
  private long slackTime = 10000;
  private long deoffloadSlackTime = 10000;


  private final int windowSize = 5;
  private final DescriptiveStatistics cpuHighAverage;
  private final DescriptiveStatistics cpuLowAverage;
  private final DescriptiveStatistics eventAverage;
  private final EvalConf evalConf;

  // DEBUGGIGN
  final ScheduledExecutorService se = Executors.newSingleThreadScheduledExecutor();

  private Map<TaskExecutor, Long> prevTaskCpuTimeMap = new HashMap<>();
  private int cpuLoadStable = 0;

  // TODO: high threshold
  // TODO: low threshold ==> threshold 2개 놓기

  private final PolynomialCpuTimeModel cpuTimeModel;

  private int observedCnt = 0;
  private final int observeWindow = 10;


  private long prevDeOffloadingTime = System.currentTimeMillis();
  final PersistentConnectionToMasterMap toMaster;

  private final String executorId;

  @Inject
  private TaskOffloader(
    @Parameter(JobConf.ExecutorId.class) String executorId,
    final SystemLoadProfiler profiler,
    @Parameter(EvalConf.BottleneckDetectionPeriod.class) final long r,
    @Parameter(EvalConf.BottleneckDetectionConsecutive.class) final int k,
    @Parameter(EvalConf.BottleneckDetectionCpuThreshold.class) final double threshold,
    final TaskEventRateCalculator taskEventRateCalculator,
    final TaskExecutorMapWrapper taskExecutorMapWrapper,
    final CpuEventModel cpuEventModel,
    final PolynomialCpuTimeModel cpuTimeModel,
    final EvalConf evalConf,
    final PersistentConnectionToMasterMap toMaster) {
    this.executorId = executorId;
    this.evalConf = evalConf;
    this.r = r;
    this.k = k;
    this.threshold = threshold;
    this.profiler = profiler;
    this.monitorThread = Executors.newSingleThreadScheduledExecutor();
    this.taskEventRateCalculator = taskEventRateCalculator;
    this.cpuTimeModel = cpuTimeModel;
    this.cpuHighAverage = new DescriptiveStatistics();
    cpuHighAverage.setWindowSize(2);
    this.cpuLowAverage = new DescriptiveStatistics();
    cpuLowAverage.setWindowSize(2);

    this.eventAverage = new DescriptiveStatistics();
    eventAverage.setWindowSize(2);

    this.taskExecutorMap = taskExecutorMapWrapper.taskExecutorMap;
    this.cpuEventModel = cpuEventModel;
    this.offloadedExecutors = new LinkedList<>();

    this.toMaster = toMaster;
  }


  private Collection<TaskExecutor> findOffloadableTasks() {
    final Set<TaskExecutor> taskExecutors = new HashSet<>(taskExecutorMap.keySet());
    for (final Pair<TaskExecutor, Long> pair : offloadedExecutors) {
      taskExecutors.remove(pair.left());
    }

    return taskExecutors;
  }

  private int calculateOFfloadedTasks() {
    int cnt = 0;
    for (final Pair<TaskExecutor, Long> offloadExecutor : offloadedExecutors) {
      if (offloadExecutor.left().isOffloaded()) {
        cnt += 1;
      }
    }
    return cnt;
  }

  private int findTasksThatProcessEvents() {
    int cnt = 0;
    for (final TaskExecutor taskExecutor : taskExecutorMap.keySet()) {
      if (taskExecutor.isRunning() || taskExecutor.isOffloadPending()) {
        cnt += 1;
      }
    }
    return cnt;
  }

  final class StatelessTaskStatInfo {
    public final int running;
    public final int offload_pending;
    public final int offloaded;
    public final int deoffloaded;
    public final int totalStateless;
    public final List<TaskExecutor> runningTasks;
    public final List<TaskExecutor> statelessRunningTasks;
    public final List<TaskExecutor> statefulRunningTasks;


    public StatelessTaskStatInfo(
      final int running, final int offload_pending, final int offloaded, final int deoffloaded,
      final int totalStateless,
      final List<TaskExecutor> runningTasks,
      final List<TaskExecutor> statelessRunningTasks,
      final List<TaskExecutor> statefulRunningTasks) {
      this.running = running;
      this.offload_pending = offload_pending;
      this.offloaded = offloaded;
      this.deoffloaded = deoffloaded;
      this.totalStateless = totalStateless;
      this.runningTasks = runningTasks;
      this.statelessRunningTasks = statelessRunningTasks;
      this.statefulRunningTasks = statefulRunningTasks;
    }

    public List<TaskExecutor> getRunningStatelessTasks() {
      return runningTasks;
    }
  }

  private StatelessTaskStatInfo measureTaskStatInfo() {
    int running = 0;
    int offpending = 0;
    int offloaded = 0;
    int deoffpending = 0;
    int stateless = 0;
    int stateful = 0;
    final List<TaskExecutor> runningTasks = new ArrayList<>(taskExecutorMap.size());
    final List<TaskExecutor> statelessRunningTasks = new ArrayList<>(taskExecutorMap.size());
    final List<TaskExecutor> statefulRunningTasks = new ArrayList<>(taskExecutorMap.size());
     for (final TaskExecutor taskExecutor : taskExecutorMap.keySet()) {
       //if (taskExecutor.isStateless()) {
       //  stateless += 1;
         if (taskExecutor.isRunning()) {
           if (taskExecutor.isStateless()) {
             stateless += 1;
             statelessRunningTasks.add(taskExecutor);
           } else {
             stateful += 1;
             statefulRunningTasks.add(taskExecutor);
           }
           runningTasks.add(taskExecutor);
           running += 1;
         } else if (taskExecutor.isOffloadPending()) {
           offpending += 1;
         } else if (taskExecutor.isOffloaded()) {
           offloaded += 1;
         } else if (taskExecutor.isDeoffloadPending()) {
           deoffpending += 1;
         }
      // }
    }

    LOG.info("Stateless Task running {}, Stateful running {}, offload_pending: {}, offloaded: {}, deoffload_pending: {}, total: {}",
      stateless, stateful, offpending, offloaded, deoffpending, taskExecutorMap.size());

     return new StatelessTaskStatInfo(running, offpending, offloaded, deoffpending, stateless, runningTasks, statelessRunningTasks, statefulRunningTasks);
  }

  private void offloading(String stageId, int time) {
    se.schedule(() -> {
      LOG.info("Start offloading {}", stageId);

      //final int offloadCnt = taskExecutorMap.keySet().stream()
      //  .filter(taskExecutor -> taskExecutor.getId().startsWith("Stage0")).toArray().length - evalConf.minVmTask;
      final int offloadCnt = taskExecutorMap.size();

      for (final TaskExecutor taskExecutor : taskExecutorMap.keySet()) {
        if (taskExecutor.getId().contains(stageId) && isTaskOffloadable(taskExecutor.getId())) {
          //LOG.info("Offload task {}, cnt: {}, offloadCnt: {}", taskExecutor.getId(), cnt, offloadCnt);
          offloadedExecutors.add(Pair.of(taskExecutor, System.currentTimeMillis()));
          taskExecutor.startOffloading(System.currentTimeMillis(), (m) -> {
            sendOffloadingDoneEvent(taskExecutor.getId());
          });
        }
      }
    }, time, TimeUnit.SECONDS);
  }

  private void deoffloading(String stageId, int time) {
    se.schedule(() -> {
      LOG.info("Start deoffloading {}", stageId);

      //final int offloadCnt = taskExecutorMap.keySet().stream()
      //  .filter(taskExecutor -> taskExecutor.getId().startsWith("Stage0")).toArray().length - evalConf.minVmTask;

      final Iterator<Pair<TaskExecutor, Long>> iterator = offloadedExecutors.iterator();
      while (iterator.hasNext()) {
        final Pair<TaskExecutor, Long> pair = iterator.next();
        if (pair.left().getId().contains(stageId)) {
          if (isTaskOffloadable(pair.left().getId())) {
            LOG.info("Deoffloading {}", pair.left().getId());
            pair.left().endOffloading((m) -> {
              LOG.info("Receive end offloading of {} ... send offloding done event", pair.left().getId());
              sendOffloadingDoneEvent(pair.left().getId());
            });
            iterator.remove();
          }
        }
      }
    }, time, TimeUnit.SECONDS);
  }

  public void startDownstreamDebugging() {
    // For offloading debugging

    offloading("Stage0", 25);

    offloading("Stage2", 65);

    deoffloading("Stage0", 95);
    deoffloading("Stage2", 110);


    /*
    offloading("Stage0", 115);

    deoffloading("Stage2", 135);
    deoffloading("Stage0", 160);
    */
  }

  public void startDebugging() {
    // For offloading debugging
    se.scheduleAtFixedRate(() -> {
      LOG.info("Start offloading kafka (only first stage)");
      int cnt = 0;

      //final int offloadCnt = taskExecutorMap.keySet().stream()
      //  .filter(taskExecutor -> taskExecutor.getId().startsWith("Stage0")).toArray().length - evalConf.minVmTask;
      final int offloadCnt = taskExecutorMap.size();

      for (final TaskExecutor taskExecutor : taskExecutorMap.keySet()) {
        if (cnt < offloadCnt) {
          LOG.info("Offload task {}, cnt: {}, offloadCnt: {}", taskExecutor.getId(), cnt, offloadCnt);
          offloadedExecutors.add(Pair.of(taskExecutor, System.currentTimeMillis()));
          taskExecutor.startOffloading(System.currentTimeMillis(), (m) -> {
            sendOffloadingDoneEvent(taskExecutor.getId());
          });
          cnt += 1;
        }
      }
    }, 25, 50, TimeUnit.SECONDS);

    se.scheduleAtFixedRate(() -> {
      LOG.info("End offloading kafka");
      while (!offloadedExecutors.isEmpty()) {
        final TaskExecutor endTask = offloadedExecutors.remove(0).left();
        LOG.info("End task {}", endTask);
        endTask.endOffloading((m) -> {
          // do sth
        });
      }
    }, 40, 50, TimeUnit.SECONDS);
  }

  private void sendOffloadingDoneEvent(final String taskId) {
    LOG.info("Send offloading done for {}", taskId);
    final CompletableFuture<ControlMessage.Message> msgFuture = toMaster
      .getMessageSender(MessageEnvironment.TASK_OFFLOADING_LISTENER_ID).request(
        ControlMessage.Message.newBuilder()
          .setId(RuntimeIdManager.generateMessageId())
          .setListenerId(MessageEnvironment.TASK_OFFLOADING_LISTENER_ID)
          .setType(ControlMessage.MessageType.RequestTaskOffloadingDone)
          .setRequestTaskOffloadingDoneMsg(ControlMessage.RequestTaskOffloadingDoneMessage.newBuilder()
            .setExecutorId(executorId)
            .setTaskId(taskId)
            .build())
          .build());
  }

  private boolean isTaskOffloadable(final String taskId) {
    final CompletableFuture<ControlMessage.Message> msgFuture = toMaster
      .getMessageSender(MessageEnvironment.TASK_OFFLOADING_LISTENER_ID).request(
        ControlMessage.Message.newBuilder()
          .setId(RuntimeIdManager.generateMessageId())
          .setListenerId(MessageEnvironment.TASK_OFFLOADING_LISTENER_ID)
          .setType(ControlMessage.MessageType.RequestTaskOffloading)
          .setRequestTaskOffloadingMsg(ControlMessage.RequestTaskOffloadingMessage.newBuilder()
            .setExecutorId(executorId)
            .setTaskId(taskId)
            .build())
          .build());

    try {
      final ControlMessage.Message msg = msgFuture.get();
      return msg.getTaskOffloadingInfoMsg().getCanOffloading();
    } catch (Exception e) {
      e.printStackTrace();
      throw new RuntimeException(e);
    }
  }

  private Map<TaskExecutor, Long> calculateCpuTimeDelta(
    final Map<TaskExecutor, Long> prevMap,
    final Map<TaskExecutor, Long> currMap) {
    final Map<TaskExecutor, Long> deltaMap = new HashMap<>(currMap);
    for (final TaskExecutor key : prevMap.keySet()) {
      final Long prevTaskTime = prevMap.get(key);
      final Long currTaskTime = currMap.get(key) == null ? 0L : currMap.get(key);
      deltaMap.put(key, currTaskTime - prevTaskTime);
    }
    return deltaMap;
  }

  private List<TaskExecutor> runningTasksInDeoffloadTimeOrder(final List<TaskExecutor> runningTasks) {
    final List<TaskExecutor> tasks = runningTasks
      .stream().filter(runningTask -> {
        return !offloadedExecutors.stream().map(Pair::left).collect(Collectors.toSet()).contains(runningTask);
      }).collect(Collectors.toList());

    tasks.sort(new Comparator<TaskExecutor>() {
      @Override
      public int compare(TaskExecutor o1, TaskExecutor o2) {
        return (int) (o1.getPrevOffloadEndTime().get() - o2.getPrevOffloadEndTime().get());
      }
    });

    return tasks;
  }


  private List<TaskExecutor> runningTasksInCpuTimeOrder(
    final List<TaskExecutor> runningTasks,
    final Map<TaskExecutor, Long> deltaMap) {

    final List<TaskExecutor> tasks = runningTasks
      .stream().filter(runningTask -> {
        return !offloadedExecutors.stream().map(Pair::left).collect(Collectors.toSet()).contains(runningTask);
      }).collect(Collectors.toList());

    tasks.sort(new Comparator<TaskExecutor>() {
      @Override
      public int compare(TaskExecutor o1, TaskExecutor o2) {
        return (int) (deltaMap.get(o2) - deltaMap.get(o1));
      }
    });

    return tasks;
  }

  public void start() {
    this.monitorThread.scheduleAtFixedRate(() -> {

      try {
        final double cpuLoad = profiler.getCpuLoad();

        final Map<TaskExecutor, Long> deltaMap =
          taskExecutorMap.keySet().stream()
          .map(taskExecutor -> {
            final long executionTime = taskExecutor.getTaskExecutionTime().get();
            taskExecutor.getTaskExecutionTime().getAndAdd(-executionTime);
            return Pair.of(taskExecutor, executionTime);
            }).collect(Collectors.toMap(Pair::left, Pair::right));

        /*
        final Map<TaskExecutor, Long> currTaskCpuTimeMap = profiler.getTaskExecutorCpuTimeMap();
        final Map<TaskExecutor, Long> deltaMap = calculateCpuTimeDelta(prevTaskCpuTimeMap, currTaskCpuTimeMap);
        prevTaskCpuTimeMap = currTaskCpuTimeMap;
        */

        //final Long elapsedCpuTimeSum = deltaMap.values().stream().reduce(0L, (x, y) -> x + y) / 1000;
        long elapsedCpuTimeSum = 0L;
        for (final Long val : deltaMap.values()) {
          elapsedCpuTimeSum += (val / 1000);
        }

        // calculate stable cpu time
        if (cpuLoad >= 0.28) {
          cpuLoadStable += 1;
          if (cpuLoadStable >= 2) {
            observedCnt += 1;
            cpuTimeModel.add(cpuLoad, elapsedCpuTimeSum);
          }
        } else {
          cpuLoadStable = 0;
        }

        cpuHighAverage.addValue(cpuLoad);
        cpuLowAverage.addValue(cpuLoad);

        final double cpuHighMean = cpuHighAverage.getMean();
        final double cpuLowMean = cpuLowAverage.getMean();

        final long currTime = System.currentTimeMillis();

        LOG.info("CPU Load: {}, Elapsed Time: {}", cpuLoad, elapsedCpuTimeSum);

        final StatelessTaskStatInfo taskStatInfo = measureTaskStatInfo();
        LOG.info("CpuHighMean: {}, CpuLowMean: {}, runningTask {}, threshold: {}, observed: {}, offloaded: {}",
          cpuHighMean, cpuLowMean, taskStatInfo.running, threshold, observedCnt);

        if (!offloadedExecutors.isEmpty()) {
          final long cur = System.currentTimeMillis();
          final Iterator<Pair<TaskExecutor, Long>> it = offloadedExecutors.iterator();
          while (it.hasNext()) {
            final Pair<TaskExecutor, Long> elem = it.next();
            if (cur - elem.right() >= TimeUnit.SECONDS.toMillis(1000)) {
              // force close!
              LOG.info("Force close workers !! {}, {}", elem.left(), elem.right());
              elem.left().endOffloading((m) -> {
                // do sth
              });
              it.remove();
              prevDeOffloadingTime = System.currentTimeMillis();
            }
          }
        }

        if (cpuHighMean > threshold && observedCnt >= observeWindow &&
          System.currentTimeMillis() - prevDeOffloadingTime >= slackTime) {

          final long targetCpuTime = 6000000;
          //cpuTimeModel
          //  .desirableMetricForLoad((threshold + evalConf.deoffloadingThreshold) / 2.0);

          // Adjust current cpu time
          // Minus the pending tasks!
          long currCpuTimeSum = 0;
          // correct
          // jst running worker
          for (final Map.Entry<TaskExecutor, Long> entry : deltaMap.entrySet()) {
            if (entry.getKey().isRunning()) {
              currCpuTimeSum  += entry.getValue() / 1000;
            }
          }

          //final long avgCpuTimePerTask = currCpuTimeSum / (taskStatInfo.running);

          LOG.info("currCpuTimeSum: {}, runningTasks: {}", currCpuTimeSum, taskStatInfo.runningTasks.size());
          //final List<TaskExecutor> runningTasks = runningTasksInDeoffloadTimeOrder(taskStatInfo.runningTasks);
          final List<TaskExecutor> runningTasks = runningTasksInCpuTimeOrder(taskStatInfo.statelessRunningTasks, deltaMap);
          final long curr = System.currentTimeMillis();
          int cnt = 0;
          for (final TaskExecutor runningTask : runningTasks) {
            final long currTaskCpuTime = deltaMap.get(runningTask) / 1000;
            //if (cnt < runningTasks.size() - 1) {

            if (curr - runningTask.getPrevOffloadEndTime().get() > slackTime &&
              isTaskOffloadable(runningTask.getId())) {
              //final long cpuTimeOfThisTask = deltaMap.get(runningTask);

              LOG.info("CurrCpuSum: {}, Task {} cpu sum: {}, targetSum: {}",
                currCpuTimeSum, runningTask.getId(), currTaskCpuTime, targetCpuTime);

              // offload this task!
              LOG.info("Offloading task {}", runningTask.getId());
              runningTask.startOffloading(System.currentTimeMillis(), (m) -> {
                sendOffloadingDoneEvent(runningTask.getId());
              });

              offloadedExecutors.add(Pair.of(runningTask, currTime));
              currCpuTimeSum -= currTaskCpuTime;

              cnt += 1;

              if (currCpuTimeSum <= targetCpuTime) {
                break;
              }
            }
            //}
          }

        } else if (cpuLowMean < evalConf.deoffloadingThreshold  &&  observedCnt >= observeWindow) {
          if (!offloadedExecutors.isEmpty()) {
            //final long targetCpuTime = cpuTimeModel.desirableMetricForLoad((threshold + evalConf.deoffloadingThreshold) / 2.0);
            final long targetCpuTime = 6000000;

            long currCpuTimeSum = 0;
            // correct
            // jst running worker
            for (final Map.Entry<TaskExecutor, Long> entry : deltaMap.entrySet()) {
              if (entry.getKey().isRunning()) {
                currCpuTimeSum  += entry.getValue() / 1000;
              } else if (entry.getKey().isDeoffloadPending()) {
                currCpuTimeSum  += entry.getKey().calculateOffloadedTaskTime();
              }
            }

            // add deoffload pending

            final Iterator<Pair<TaskExecutor, Long>> iterator = offloadedExecutors.iterator();
            while (iterator.hasNext() && currCpuTimeSum < targetCpuTime) {
              final Pair<TaskExecutor, Long> pair = iterator.next();
              final TaskExecutor taskExecutor = pair.left();
              if (taskExecutor.isOffloaded()) {
                final Long offloadingTime = taskExecutor.getPrevOffloadStartTime().get();
                final long avgCpuTimeSum = taskExecutor.calculateOffloadedTaskTime();

                if (avgCpuTimeSum > 0) {
                  LOG.info("Deoff] CurrCpuSum: {}, Task {} avg cpu sum: {}, targetSum: {}",
                    currCpuTimeSum, taskExecutor.getId(), avgCpuTimeSum, targetCpuTime);

                  if (currTime - offloadingTime >= deoffloadSlackTime
                     && isTaskOffloadable(taskExecutor.getId())) {
                    LOG.info("Deoffloading task {}, currCpuTime: {}, avgCpuSUm: {}",
                      taskExecutor.getId(), currCpuTimeSum, avgCpuTimeSum);
                    iterator.remove();
                    taskExecutor.endOffloading((m) -> {
                      // do sth
                      sendOffloadingDoneEvent(taskExecutor.getId());
                    });
                    currCpuTimeSum += avgCpuTimeSum;
                    prevDeOffloadingTime = System.currentTimeMillis();
                  }
                }
              } else if (taskExecutor.isOffloadPending()) {
                /*
                // pending means that it is not offloaded yet.
                // close immediately!
                LOG.info("Immediately deoffloading!");
                taskExecutor.endOffloading((m) -> {
                  // do sth
                });
                iterator.remove();
                */
              }
            }
          }
        }
      } catch (final Exception e) {
        e.printStackTrace();
        throw new RuntimeException(e);
      }
    }, r, r, TimeUnit.MILLISECONDS);
  }

  public void close() {
    monitorThread.shutdown();
  }


  // 어느 시점 (baseTime) 을 기준으로 fluctuation 하였는가?
  private boolean isBursty(final long baseTime,
                           final List<Pair<Long, Double>> processedEvents) {
    final List<Double> beforeBaseTime = new ArrayList<>();
    final List<Double> afterBaseTime = new ArrayList<>();

    for (final Pair<Long, Double> pair : processedEvents) {
      if (pair.left() < baseTime) {
        beforeBaseTime.add(pair.right());
      } else {
        afterBaseTime.add(pair.right());
      }
    }

    final double avgEventBeforeBaseTime = beforeBaseTime.stream()
      .reduce(0.0, (x, y) -> x + y) / (Math.max(1, beforeBaseTime.size()));

    final double avgEventAfterBaseTime = afterBaseTime.stream()
      .reduce(0.0, (x, y) -> x + y) / (Math.max(1, afterBaseTime.size()));

    LOG.info("avgEventBeforeBaseTime: {} (size: {}), avgEventAfterBaseTime: {} (size: {}), baseTime: {}",
      avgEventBeforeBaseTime, beforeBaseTime.size(), avgEventAfterBaseTime, afterBaseTime.size(), baseTime);

    processedEvents.clear();

    if (avgEventBeforeBaseTime * 2 < avgEventAfterBaseTime) {
      return true;
    } else {
      return false;
    }
  }
}
