package org.apache.nemo.runtime.executor.offloading;

import org.apache.commons.lang3.tuple.Triple;
import org.apache.nemo.conf.EvalConf;
import org.apache.nemo.conf.JobConf;
import org.apache.nemo.offloading.common.TaskHandlingEvent;
import org.apache.nemo.runtime.executor.NettyStateStore;
import org.apache.nemo.runtime.executor.PipeIndexMapWorker;
import org.apache.nemo.runtime.executor.TaskExecutorMapWrapper;
import org.apache.nemo.runtime.executor.bytetransfer.ByteTransport;
import org.apache.nemo.runtime.executor.common.controlmessages.TaskControlMessage;
import org.apache.nemo.runtime.lambdaexecutor.NetworkUtils;
import org.apache.reef.tang.annotations.Parameter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;


public final class MultipleWorkersMergingOffloadingManagerImpl extends AbstractOffloadingManagerImpl {
  private static final Logger LOG = LoggerFactory.getLogger(MultipleWorkersMergingOffloadingManagerImpl.class.getName());


  @Inject
  private MultipleWorkersMergingOffloadingManagerImpl(final OffloadingWorkerFactory workerFactory,
                                                      final TaskExecutorMapWrapper taskExecutorMapWrapper,
                                                      final EvalConf evalConf,
                                                      final PipeIndexMapWorker pipeIndexMapWorker,
                                                      @Parameter(JobConf.ExecutorId.class) final String executorId,
                                                      final ByteTransport byteTransport,
                                                      final NettyStateStore nettyStateStore) {
    super(workerFactory, taskExecutorMapWrapper, evalConf, pipeIndexMapWorker, executorId,
      NetworkUtils.getPublicIP(), nettyStateStore.getPort(), true,
      evalConf.destroyOffloadingWorker);
  }


  @Override
  public void createWorkers(String taskId) {
    final List<OffloadingWorker> worker = createWorkerBlocking(1);
    synchronized (workers) {
      workers.add(worker.get(0));
      taskWorkerMap.put(taskId, worker);
      workerTaskMap.put(worker.get(0), new LinkedList<>(Arrays.asList(taskId)));
    }
  }

  private int cnt = 0;

  @Override
  Optional<List<OffloadingWorker>> selectWorkersForOffloading(String taskId) {
    offloadingStart = System.currentTimeMillis();
    rrSchedulingMap.putIfAbsent(taskId, new AtomicInteger(0));
    deoffloadedMap.put(taskId, new AtomicBoolean(false));
    sendTask.put(taskId, new AtomicBoolean(false));

    synchronized (workers) {
      LOG.info("Size of workers: {}, cnt: {}, task: {}", workers.size(), cnt, taskId);

      if (taskWorkerMap.containsKey(taskId) && taskWorkerMap.get(taskId).size() > 0) {
        return Optional.of(taskWorkerMap.get(taskId));
      } else {
        final int startIndex = cnt % workers.size();
        final int endIndex = (cnt + evalConf.numOffloadingWorker) % (workers.size() + 1);

        final List<OffloadingWorker> selectedWorkers;
        if (endIndex < startIndex) {
          selectedWorkers = new ArrayList<>(evalConf.numOffloadingWorker);
          for (int i = startIndex; i < workers.size(); i++) {
            selectedWorkers.add(workers.get(i));
          }
          for (int i = 0; i < endIndex; i++) {
            selectedWorkers.add(workers.get(i));
          }
        } else {
          selectedWorkers =
            new LinkedList<>(workers.subList(startIndex, endIndex));
        }

        taskWorkerMap.put(taskId, selectedWorkers);
        selectedWorkers.forEach(worker -> {
          workerTaskMap.put(worker, new LinkedList<>(Arrays.asList(taskId)));
        });

        cnt += evalConf.numOffloadingWorker;

        return Optional.of(taskWorkerMap.get(taskId));
      }
    }
  }

  private final Map<String, AtomicInteger> rrSchedulingMap = new ConcurrentHashMap<>();

  private long offloadingStart;
  private final Map<String, AtomicBoolean> deoffloadedMap = new ConcurrentHashMap<>();

  private final Map<String, AtomicBoolean> sendTask = new ConcurrentHashMap<>();

  @Override
  Optional<OffloadingWorker> selectWorkerForIntermediateOffloading(String taskId, TaskHandlingEvent data) {

    if (!deoffloadedMap.get(taskId).get() && System.currentTimeMillis() - offloadingStart >= 21000) {
      synchronized (deoffloadedMap.get(taskId)) {
        if (!deoffloadedMap.get(taskId).get()) {
          deoffloadedMap.get(taskId).set(true);

          final Triple<String, String, String> key = pipeIndexMapWorker.getIndexMap().keySet().stream()
            .filter(k -> k.getRight().equals(taskId)).collect(Collectors.toList()).get(0);
          final int pipeIndex = pipeIndexMapWorker.getPipeIndex(key.getLeft(), key.getMiddle(), key.getRight());

          LOG.info("Sending deoffloading for task ${} to decrease number of workers down to {}, " +
            "total workers {}", taskId, evalConf.numOffloadingWorkerAfterMerging, taskWorkerMap.get(taskId).size());
          synchronized (taskWorkerMap.get(taskId)) {

            final List<OffloadingWorker> common = findCommonWorkersToOffloadTask(taskWorkerMap.get(taskId));

            for (final OffloadingWorker worker : taskWorkerMap.get(taskId)) {
              if (!common.contains(worker)) {
                LOG.info("Send deoffloading message for task {} to worker index {}", taskId, worker.getId());
                worker.writeData
                  (pipeIndex,
                    new TaskControlMessage(TaskControlMessage.TaskControlMessageType.OFFLOAD_TASK_STOP,
                      pipeIndex,
                      pipeIndex,
                      taskId, null));
              }
            }
          }
        }
      }
    }

    if (System.currentTimeMillis() - offloadingStart >= 20000) {
      // global workers

      final AtomicBoolean ab = sendTask.get(taskId);

      if (ab.compareAndSet(false, true)) {
        // send task
        LOG.info("Send task to common worker {}", taskId);
        final List<OffloadingWorker> common = findCommonWorkersToOffloadTask(taskWorkerMap.get(taskId));
        if (!common.isEmpty()) {

          taskWorkerMap.get(taskId).addAll(common);
          common.forEach(worker -> {
            workerTaskMap.get(worker).add(taskId);
          });

          offloadTaskToWorker(taskId, common, false);
        }
      }

      final int index = rrSchedulingMap.get(taskId).getAndIncrement() % evalConf.numOffloadingWorkerAfterMerging;
      final List<OffloadingWorker> l = taskWorkerMap.get(taskId);
      return Optional.of(l.get(index));
    } else {
      final List<OffloadingWorker> l = taskWorkerMap.get(taskId);
      final int index = rrSchedulingMap.get(taskId).getAndIncrement() % evalConf.numOffloadingWorker;

      return Optional.of(l.get(index));
    }
  }

  private List<OffloadingWorker> findCommonWorkersToOffloadTask(final List<OffloadingWorker> myWorkers) {
    final List<OffloadingWorker> common = workers.subList(0, evalConf.numOffloadingWorkerAfterMerging);
    final List<OffloadingWorker> result = new ArrayList<>(common.size());

    for (final OffloadingWorker w : common) {
      if (!myWorkers.contains(w)) {
        result.add(w);
      }
    }

    return result;
  }

  @Override
  Optional<OffloadingWorker> selectWorkerForSourceOffloading(String taskId, Object data) {
    return Optional.of(taskWorkerMap.get(taskId).get(0));
  }

}
