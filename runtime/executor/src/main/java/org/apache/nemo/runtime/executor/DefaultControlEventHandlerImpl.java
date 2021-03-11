package org.apache.nemo.runtime.executor;

import org.apache.nemo.common.RuntimeIdManager;
import org.apache.nemo.conf.EvalConf;
import org.apache.nemo.conf.JobConf;
import org.apache.nemo.runtime.common.comm.ControlMessage;
import org.apache.nemo.runtime.executor.common.ControlEventHandler;
import org.apache.nemo.runtime.executor.common.TaskExecutor;
import org.apache.nemo.offloading.common.TaskHandlingEvent;
import org.apache.nemo.runtime.executor.common.controlmessages.TaskControlMessage;
import org.apache.nemo.runtime.executor.common.datatransfer.PipeManagerWorker;
import org.apache.nemo.runtime.message.PersistentConnectionToMasterMap;
import org.apache.reef.tang.annotations.Parameter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;

import static org.apache.nemo.runtime.message.MessageEnvironment.ListenerType.RUNTIME_MASTER_MESSAGE_LISTENER_ID;


public final class DefaultControlEventHandlerImpl implements ControlEventHandler {
  private static final Logger LOG = LoggerFactory.getLogger(DefaultControlEventHandlerImpl.class.getName());

  private final String executorId;
  private final TaskExecutorMapWrapper taskExecutorMapWrapper;
  private final PipeManagerWorker pipeManagerWorker;
  private final PersistentConnectionToMasterMap toMaster;
  private final EvalConf evalConf;

  @Inject
  private DefaultControlEventHandlerImpl(
    @Parameter(JobConf.ExecutorId.class) final String executorId,
    final TaskExecutorMapWrapper taskExecutorMapWrapper,
    final PipeManagerWorker pipeManagerWorker,
    final EvalConf evalConf,
    final PersistentConnectionToMasterMap toMaster) {
    this.executorId = executorId;
    this.taskExecutorMapWrapper = taskExecutorMapWrapper;
    this.pipeManagerWorker = pipeManagerWorker;
    this.toMaster = toMaster;
    this.evalConf = evalConf;
  }

  @Override
  public void handleControlEvent(TaskHandlingEvent event) {
    final TaskControlMessage control = (TaskControlMessage) event.getControl();
    switch (control.type) {
      case TASK_STOP_SIGNAL_BY_MASTER: {
        final TaskExecutor taskExecutor = taskExecutorMapWrapper.getTaskExecutor(control.getTaskId());

        if (taskExecutor.isSource()) {
          if (!pipeManagerWorker.isOutputPipeStopped(taskExecutor.getId())) {
            // stop and remove task now
            // there is no pending event
            pipeManagerWorker.setTaskStop(taskExecutor.getId());
            stopAndCheckpointTask(taskExecutor.getId());
          }
        } else {
          // Stop input pipe
          taskExecutor.getTask().getUpstreamTasks().entrySet().forEach(entry -> {
            pipeManagerWorker.sendStopSignalForInputPipes(entry.getValue(),
              entry.getKey().getId(), control.getTaskId());
          });
        }
        break;
      }
      case PIPE_OUTPUT_STOP_SIGNAL_BY_DOWNSTREAM_TASK: {
        // do not send data any more
        final int pipeIndex = control.targetPipeIndex;
        if (evalConf.controlLogging) {
          LOG.info("Receive PIPE_OUTPUT_STOP_SIGNAL_BY_DOWNSTREAM_TASK of index {} for task {} in executor {}", pipeIndex, control.getTaskId(), executorId);
        }
        pipeManagerWorker.stopOutputPipe(pipeIndex, control.getTaskId());
        break;
      }
      case PIPE_OUTPUT_STOP_ACK_FROM_UPSTREAM_TASK: {
        pipeManagerWorker.receiveAckInputStopSignal(control.getTaskId(), control.targetPipeIndex);

        if (canTaskMoved(control.getTaskId())) {
          stopAndCheckpointTask(control.getTaskId());
        }
        break;
      }
      case PIPE_INIT: {

        if (evalConf.controlLogging) {
          LOG.info("Pipe init message, targetIndex: {}, targetTask {}, in executor {}", control.targetPipeIndex, control.getTaskId(), executorId);
        }
        pipeManagerWorker.startOutputPipe(control.targetPipeIndex, control.getTaskId());

        if (canTaskMoved(control.getTaskId())) {
          if (evalConf.controlLogging) {
            LOG.info("Task can be moved {}, inputStateStopped {}, isOutputStoped: {}",
              control.getTaskId(), pipeManagerWorker.isInputPipeStopped(control.getTaskId()), pipeManagerWorker.isOutputPipeStopped(control.getTaskId())
            );
          }
          stopAndCheckpointTask(control.getTaskId());
        }
        break;
      }
      default:
        throw new RuntimeException("Invalid control message type " + control.type);
    }
  }

  private boolean canTaskMoved(final String taskId) {
    // output stopped means that it is waiting for moving downstream task
    return pipeManagerWorker.isInputPipeStopped(taskId)
      && !pipeManagerWorker.isOutputPipeStopped(taskId);
  }

  private void stopAndCheckpointTask(final String taskId) {
    // stop and remove task
    final TaskExecutor taskExecutor = taskExecutorMapWrapper.getTaskExecutor(taskId);
    taskExecutor.checkpoint(true);

    taskExecutorMapWrapper.removeTask(taskId);

    toMaster.getMessageSender(RUNTIME_MASTER_MESSAGE_LISTENER_ID)
      .send(ControlMessage.Message.newBuilder()
        .setId(RuntimeIdManager.generateMessageId())
        .setListenerId(RUNTIME_MASTER_MESSAGE_LISTENER_ID.ordinal())
          .setType(ControlMessage.MessageType.StopTaskDone)
          .setStopTaskDoneMsg(ControlMessage.StopTaskDoneMessage.newBuilder()
            .setExecutorId(executorId)
            .setTaskId(taskId)
            .build())
          .build());
  }
}
