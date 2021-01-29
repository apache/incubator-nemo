package org.apache.nemo.runtime.executor.vmscaling;

import org.apache.nemo.common.Pair;
import org.apache.nemo.common.TaskMetrics;
import org.apache.nemo.offloading.common.EventHandler;
import org.apache.nemo.runtime.executor.SFTaskMetrics;
import org.apache.nemo.runtime.executor.StageOffloadingWorkerManager;
import org.apache.nemo.runtime.executor.common.OffloadingDoneEvent;
import org.apache.nemo.runtime.executor.common.TaskExecutor;
import org.apache.nemo.runtime.executor.task.DefaultTaskExecutorImpl;
import org.apache.nemo.runtime.lambdaexecutor.OffloadingHeartbeatEvent;
import org.apache.nemo.runtime.lambdaexecutor.OffloadingResultEvent;
import org.apache.nemo.runtime.lambdaexecutor.StateOutput;
import org.apache.nemo.runtime.lambdaexecutor.ThpEvent;
import org.apache.nemo.runtime.lambdaexecutor.kafka.KafkaOffloadingOutput;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class VMScalingWorkerEventHandler implements EventHandler {

  private static final Logger LOG = LoggerFactory.getLogger(VMScalingWorkerEventHandler.class.getName());

  private final TaskExecutor te;
  private final SFTaskMetrics sfTaskMetrics;
  private final String newExecutorId;
  private final String stageId;
  private final StageOffloadingWorkerManager stageOffloadingWorkerManager;

  public VMScalingWorkerEventHandler(final TaskExecutor te,
                                     final SFTaskMetrics sfTaskMetrics,
                                     final String newExecutorId,
                                     final String stageId,
                                     final StageOffloadingWorkerManager stageOffloadingWorkerManager) {
    this.te = te;
    this.sfTaskMetrics = sfTaskMetrics;
    this.newExecutorId = newExecutorId;
    this.stageId = stageId;
    this.stageOffloadingWorkerManager = stageOffloadingWorkerManager;
  }

  @Override
  public void onNext(Object msg) {
    // TODO: We should retrieve states (checkpointmark, operator states, and so on)

    LOG.info("Message from vm worker for task {}, {}", te.getId(), msg);

    if (msg instanceof OffloadingHeartbeatEvent) {
      final OffloadingHeartbeatEvent heartbeatEvent = (OffloadingHeartbeatEvent) msg;
      // LOG.info("Set task metrics from vm-scaling", heartbeatEvent.taskMetrics);

      for (final Pair<String, TaskMetrics.RetrievedMetrics> metric : heartbeatEvent.taskMetrics) {
        sfTaskMetrics.sfTaskMetrics.put(metric.left(), metric.right());
      }

      sfTaskMetrics.cpuLoadMap.put(heartbeatEvent.executorId, heartbeatEvent.cpuUse);

    } else if (msg instanceof OffloadingResultEvent) {
      if (((OffloadingResultEvent) msg).data.size() > 0) {
        // LOG.info("Result received from vm-scaling: cnt {}", ((OffloadingResultEvent) msg).data.size());
        // te.handleOffloadingEvent(msg);
      }
    } else if (msg instanceof ThpEvent) {
      // just print the log
      final ThpEvent thpEvent = (ThpEvent) msg;
      LOG.info("Thp: {} at {}/{} from vm-scaling", thpEvent.thp, thpEvent.opId, thpEvent.taskId);
    } else if (msg instanceof OffloadingDoneEvent) {
      final OffloadingDoneEvent e = (OffloadingDoneEvent) msg;
       LOG.info("Offloading done event {}", e.taskId);
    } else if (msg instanceof KafkaOffloadingOutput) {
      final KafkaOffloadingOutput e = (KafkaOffloadingOutput) msg;

      LOG.info("Offloading done event (kafkaOffloadingOutput) {}", e.taskId);

      ((DefaultTaskExecutorImpl) te).offloader.get()
        .handleOffloadingOutput(e);

      // handle stageOffloadingDone
      stageOffloadingWorkerManager.endOffloading(stageId);

    } else if (msg instanceof StateOutput) {
      final StateOutput e = (StateOutput) msg;

      LOG.info("Offloading done event (StateOutput) {}", e.taskId);

      ((DefaultTaskExecutorImpl) te).offloader.get()
        .handleStateOutput((StateOutput) msg);

      // handle stageOffloadingDone
      stageOffloadingWorkerManager.endOffloading(stageId);

    } else {

      LOG.info("Handling offloading event for {}: {}", te.getId(), msg);
      // te.handleOffloadingEvent(msg);
    }
  }
}
