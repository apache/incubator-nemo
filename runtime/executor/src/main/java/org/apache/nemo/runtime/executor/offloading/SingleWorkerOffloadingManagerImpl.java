package org.apache.nemo.runtime.executor.offloading;

import org.apache.nemo.conf.EvalConf;
import org.apache.nemo.conf.JobConf;
import org.apache.nemo.offloading.client.NetworkUtils;
import org.apache.nemo.offloading.common.TaskHandlingEvent;
import org.apache.nemo.runtime.executor.NettyStateStore;
import org.apache.nemo.runtime.executor.PipeIndexMapWorker;
import org.apache.nemo.runtime.executor.TaskExecutorMapWrapper;
import org.apache.nemo.runtime.executor.bytetransfer.ByteTransport;
import org.apache.reef.tang.annotations.Parameter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import java.util.*;


public final class SingleWorkerOffloadingManagerImpl extends AbstractOffloadingManagerImpl {
  private static final Logger LOG = LoggerFactory.getLogger(SingleWorkerOffloadingManagerImpl.class.getName());


  @Inject
  private SingleWorkerOffloadingManagerImpl(final OffloadingWorkerFactory workerFactory,
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
  synchronized Optional<List<OffloadingWorker>> selectWorkersForOffloading(String taskId) {
    synchronized (workers) {
      cnt += 1;

      if (taskWorkerMap.containsKey(taskId) && taskWorkerMap.get(taskId).size() > 0) {
        return Optional.of(taskWorkerMap.get(taskId));
      } else {
        int index = cnt % workers.size();
        taskWorkerMap.putIfAbsent(taskId, new LinkedList<>());
        taskWorkerMap.get(taskId).add(workers.get(index));

        workerTaskMap.putIfAbsent(workers.get(index), new LinkedList<>());
        workerTaskMap.get(workers.get(index)).add(taskId);
        return Optional.of(taskWorkerMap.get(taskId));
      }

    }
  }

  @Override
  Optional<OffloadingWorker> selectWorkerForIntermediateOffloading(String taskId, TaskHandlingEvent data) {
    return Optional.of(taskWorkerMap.get(taskId).get(0));
  }

  @Override
  Optional<OffloadingWorker> selectWorkerForSourceOffloading(String taskId, Object data) {
    return Optional.of(taskWorkerMap.get(taskId).get(0));
  }

}
