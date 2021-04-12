package org.apache.nemo.runtime.executor.common;

import org.apache.nemo.conf.EvalConf;
import org.apache.nemo.conf.JobConf;
import org.apache.nemo.runtime.common.comm.ControlMessage;
import org.apache.nemo.runtime.executor.common.ControlEventHandler;
import org.apache.nemo.runtime.executor.common.ExecutorMetrics;
import org.apache.nemo.runtime.executor.common.ExecutorThread;
import org.apache.nemo.runtime.message.MessageSender;
import org.apache.nemo.runtime.message.PersistentConnectionToMasterMap;
import org.apache.reef.tang.annotations.Parameter;

import javax.inject.Inject;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

import static org.apache.nemo.runtime.message.MessageEnvironment.ListenerType.TASK_SCHEDULE_MAP_LISTENER_ID;

public final class ExecutorThreads {

  private final List<ExecutorThread> executorThreads;
  private final ExecutorMetrics executorMetrics;

  @Inject
  private ExecutorThreads(@Parameter(EvalConf.ExecutorThreadNum.class) final int threadNum,
                          @Parameter(JobConf.ExecutorId.class) final String executorId,
                          final ControlEventHandler taskControlEventHandler,
                          final PersistentConnectionToMasterMap persistentConnectionToMasterMap,
                          final MetricMessageSender metricMessageSender,
                          final TaskExecutorMapWrapper taskExecutorMapWrapper,
                          final ExecutorMetrics executorMetrics) {
    final MessageSender<ControlMessage.Message> taskScheduledMapSender =
      persistentConnectionToMasterMap.getMessageSender(TASK_SCHEDULE_MAP_LISTENER_ID);

    this.executorMetrics = executorMetrics;
    this.executorThreads = new ArrayList<>(threadNum);
    for (int i = 0; i < threadNum; i++) {
      executorThreads.add(new ExecutorThread(
        i, executorId, taskControlEventHandler, Long.MAX_VALUE, executorMetrics,
        persistentConnectionToMasterMap, metricMessageSender,
        taskScheduledMapSender, taskExecutorMapWrapper, false));
      executorThreads.get(i).start();
    }
  }

  public List<ExecutorThread> getExecutorThreads() {
    return executorThreads;
  }
}
