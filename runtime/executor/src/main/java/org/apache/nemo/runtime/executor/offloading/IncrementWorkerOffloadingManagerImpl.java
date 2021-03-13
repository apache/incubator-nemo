package org.apache.nemo.runtime.executor.offloading;

import org.apache.nemo.conf.EvalConf;
import org.apache.nemo.conf.JobConf;
import org.apache.nemo.common.NetworkUtils;
import org.apache.nemo.offloading.common.TaskHandlingEvent;
import org.apache.nemo.runtime.common.NettyStateStore;
import org.apache.nemo.runtime.executor.common.PipeIndexMapWorker;
import org.apache.nemo.runtime.executor.common.TaskExecutorMapWrapper;
import org.apache.nemo.runtime.executor.common.ExecutorMetrics;
import org.apache.reef.tang.annotations.Parameter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;


public final class IncrementWorkerOffloadingManagerImpl extends AbstractOffloadingManagerImpl {
  private static final Logger LOG = LoggerFactory.getLogger(IncrementWorkerOffloadingManagerImpl.class.getName());


  @Inject
  private IncrementWorkerOffloadingManagerImpl(final OffloadingWorkerFactory workerFactory,
                                               final TaskExecutorMapWrapper taskExecutorMapWrapper,
                                               final EvalConf evalConf,
                                               final PipeIndexMapWorker pipeIndexMapWorker,
                                               @Parameter(JobConf.ExecutorId.class) final String executorId,
                                               final NettyStateStore nettyStateStore) {
    super(workerFactory, taskExecutorMapWrapper, evalConf, pipeIndexMapWorker, executorId,
      NetworkUtils.getPublicIP(), nettyStateStore.getPort(), false,
      evalConf.destroyOffloadingWorker);
  }


  private boolean isOverloaded(final OffloadingWorker worker) {
    if (!worker.getExecutorMetrics().isPresent()) {
      return false;
    }

    final ExecutorMetrics executorMetrics = (ExecutorMetrics) worker.getExecutorMetrics().get();
    long inSum = 0L;
    long outSum = 0L;
    for (final String key : executorMetrics.taskInputProcessRateMap.keySet()) {
      inSum += executorMetrics.taskInputProcessRateMap.get(key).left().get();
      outSum += executorMetrics.taskInputProcessRateMap.get(key).right().get();
    }

    if (outSum == 0) {
      return false;
    } else {
      return (inSum / (double) outSum) > 1.5;
    }
  }

  @Override
  public void createWorkers(String taskId) {
    // Find available workers
    synchronized (workers) {
      // final List<OffloadingWorker> offloadingWorkers = workers.stream().filter(worker ->
      //  worker.getExecutorMetrics().isPresent() && !isOverloaded(worker))
      //  .collect(Collectors.toList());

      LOG.info("Creating new worker for offloading...!!");
      final List<OffloadingWorker> newWorkers = createWorkerBlocking(1);

      workers.addAll(newWorkers);
      taskWorkerMap.put(taskId, newWorkers);
      newWorkers.forEach( worker -> {
        workerTaskMap.put(worker, new LinkedList<>(Arrays.asList(taskId)));
      });
    }
  }

  @Override
  Optional<List<OffloadingWorker>> selectWorkersForOffloading(String taskId) {
    synchronized (workers) {
      if (taskWorkerMap.containsKey(taskId) && taskWorkerMap.get(taskId).size() > 0) {
        return Optional.of(taskWorkerMap.get(taskId));
      } else {
        return Optional.empty();
      }
    }
  }

  @Override
  Optional<OffloadingWorker> selectWorkerForIntermediateOffloading(String taskId, TaskHandlingEvent data) {
    return selectWorkerForOffloading(taskId);
  }

  @Override
  Optional<OffloadingWorker> selectWorkerForSourceOffloading(String taskId, Object data) {
    return selectWorkerForOffloading(taskId);
  }

  private final Map<String, AtomicInteger> taskPendingWorkerCounterMap = new ConcurrentHashMap<>();

  private Optional<OffloadingWorker> selectWorkerForOffloading(String taskId) {
    final List<OffloadingWorker> offWorkers =
      taskWorkerMap.get(taskId).stream().filter(worker -> {
        return worker.hasReadyTask(taskId) && !isOverloaded(worker);
      }).collect(Collectors.toList());

    final List<OffloadingWorker> overloadWorkers =
      taskWorkerMap.get(taskId).stream().filter(worker -> {
        return worker.hasReadyTask(taskId) && isOverloaded(worker);
      }).collect(Collectors.toList());

    taskPendingWorkerCounterMap.putIfAbsent(taskId, new AtomicInteger());

    if (offWorkers.isEmpty()) {

      if (taskPendingWorkerCounterMap.get(taskId).getAndIncrement() == 0) {
        executorService.execute(() -> {
          final double rate = overloadWorkers.get(0).getProcessingRate();
          final long currBuffered = currBufferedData.get();

          final List<OffloadingWorker> newWorkers = createWorkerBlocking((int) Math.ceil(currBuffered / (double)rate));
          LOG.info("Creating new worker for offloading... buffered {} rate {} !! {}",
            currBuffered, rate, newWorkers);
          workers.addAll(newWorkers);
          taskWorkerMap.get(taskId).addAll(newWorkers);
          newWorkers.forEach(worker -> {
            workerTaskMap.put(worker, new LinkedList<>(Arrays.asList(taskId)));
          });

          offloadTaskToWorker(taskId, newWorkers, true);
          LOG.info("Offload task for new worker {} , task {}", newWorkers, taskId);
          taskPendingWorkerCounterMap.get(taskId).decrementAndGet();
          // createWorkerBlocking(taskWorkerMap.get(taskId).size() * 2);
        });
      } else {
        taskPendingWorkerCounterMap.get(taskId).decrementAndGet();
      }
      return Optional.empty();
    }

    return Optional.of(selectLeastLoadWorker(offWorkers));
  }


  private final Random random = new Random();

  private OffloadingWorker selectLeastLoadWorker(final List<OffloadingWorker> offWorkers) {
    final int index = random.nextInt(offWorkers.size());
    return offWorkers.get(index);

    /*
    offWorkers.sort(new Comparator<OffloadingWorker>() {
      @Override
      public int compare(OffloadingWorker o1, OffloadingWorker o2) {
        final ExecutorMetrics em1 = (ExecutorMetrics) o1.getExecutorMetrics().get();
        final ExecutorMetrics em2 = (ExecutorMetrics) o2.getExecutorMetrics().get();
        return (int) (em1.load - em2.load);
      }
    });
    // LOG.info("Selected leaset load worker {} / workers {} / taskWorkerMap: {} ", offWorkers.get(0).getId(), offWorkers,
    //  taskWorkerMap);
    return offWorkers.get(0);
    */
  }
}
