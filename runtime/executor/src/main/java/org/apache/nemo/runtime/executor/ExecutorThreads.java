package org.apache.nemo.runtime.executor;

import org.apache.nemo.conf.EvalConf;
import org.apache.nemo.conf.JobConf;
import org.apache.nemo.runtime.executor.common.ControlEventHandler;
import org.apache.nemo.runtime.executor.common.ExecutorMetrics;
import org.apache.nemo.runtime.executor.common.ExecutorThread;
import org.apache.reef.tang.annotations.Parameter;

import javax.inject.Inject;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

public final class ExecutorThreads {

  private final List<ExecutorThread> executorThreads;
  private final ExecutorMetrics executorMetrics;

  @Inject
  private ExecutorThreads(final EvalConf evalConf,
                          @Parameter(JobConf.ExecutorId.class) final String executorId,
                          final ControlEventHandler taskControlEventHandler,
                          final ExecutorMetrics executorMetrics) {

    this.executorMetrics = executorMetrics;
    this.executorThreads = new ArrayList<>(evalConf.executorThreadNum);
    for (int i = 0; i < evalConf.executorThreadNum; i++) {
      executorThreads.add(new ExecutorThread(
        i, executorId, taskControlEventHandler, Long.MAX_VALUE, executorMetrics, false));
      executorThreads.get(i).start();
    }
  }

  public List<ExecutorThread> getExecutorThreads() {
    return executorThreads;
  }
}
