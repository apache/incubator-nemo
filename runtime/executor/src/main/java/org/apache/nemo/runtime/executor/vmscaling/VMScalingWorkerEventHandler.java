package org.apache.nemo.runtime.executor.vmscaling;

import org.apache.nemo.common.Pair;
import org.apache.nemo.common.TaskMetrics;
import org.apache.nemo.offloading.common.EventHandler;
import org.apache.nemo.runtime.executor.SFTaskMetrics;
import org.apache.nemo.runtime.executor.common.OffloadingDoneEvent;
import org.apache.nemo.runtime.executor.common.TaskExecutor;
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

  public VMScalingWorkerEventHandler(final TaskExecutor te,
                                     final SFTaskMetrics sfTaskMetrics,
                                     final String newExecutorId) {
    this.te = te;
    this.sfTaskMetrics = sfTaskMetrics;
    this.newExecutorId = newExecutorId;
  }

  @Override
  public void onNext(Object event) {
    // TODO: We should retrieve states (checkpointmark, operator states, and so on)
    final Pair<String, Object> pair = (Pair<String, Object>) event;
    final Object msg = pair.right();

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
        te.handleOffloadingEvent(msg);
      }
    } else if (msg instanceof ThpEvent) {
      // just print the log
      final ThpEvent thpEvent = (ThpEvent) msg;
      LOG.info("Thp: {} at {}/{} from vm-scaling", thpEvent.thp, thpEvent.opId, thpEvent.taskId);
    } else if (msg instanceof KafkaOffloadingOutput || msg instanceof StateOutput) {
      // End of the task!
      te.handleOffloadingEvent(msg);
    } else if (msg instanceof OffloadingDoneEvent) {
      final OffloadingDoneEvent e = (OffloadingDoneEvent) msg;
      // LOG.info("Moving task {} to vm {} done of {}", e.taskId, newExecutorId);
    } else {
      te.handleOffloadingEvent(msg);
    }
  }
}
