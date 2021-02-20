package org.apache.nemo.runtime.executor.offloading;

import org.apache.nemo.conf.EvalConf;
import org.apache.nemo.conf.JobConf;
import org.apache.nemo.offloading.common.TaskHandlingEvent;
import org.apache.nemo.runtime.executor.NettyStateStore;
import org.apache.nemo.runtime.executor.PipeIndexMapWorker;
import org.apache.nemo.runtime.executor.TaskExecutorMapWrapper;
import org.apache.nemo.runtime.executor.bytetransfer.ByteTransport;
import org.apache.nemo.runtime.lambdaexecutor.NetworkUtils;
import org.apache.reef.tang.annotations.Parameter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;


public final class SingleTaskMultipleWorkersOffloadingManagerImpl extends AbstractOffloadingManagerImpl {
  private static final Logger LOG = LoggerFactory.getLogger(SingleTaskMultipleWorkersOffloadingManagerImpl.class.getName());


  @Inject
  private SingleTaskMultipleWorkersOffloadingManagerImpl(final OffloadingWorkerFactory workerFactory,
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
    rrSchedulingMap.putIfAbsent(taskId, new AtomicInteger(0));

    synchronized (workers) {

      if (taskWorkerMap.containsKey(taskId) && taskWorkerMap.get(taskId).size() > 0) {
        return Optional.of(taskWorkerMap.get(taskId));
      } else {
        final List<OffloadingWorker> selectedWorkers = workers.subList(cnt, cnt + evalConf.numOffloadingWorker);
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

  @Override
  Optional<OffloadingWorker> selectWorkerForIntermediateOffloading(String taskId, TaskHandlingEvent data) {
    final int index = rrSchedulingMap.get(taskId).getAndIncrement() % taskWorkerMap.get(taskId).size();
    return Optional.of(taskWorkerMap.get(taskId).get(index));
  }

  @Override
  Optional<OffloadingWorker> selectWorkerForSourceOffloading(String taskId, Object data) {
    return Optional.of(taskWorkerMap.get(taskId).get(0));
  }

}
