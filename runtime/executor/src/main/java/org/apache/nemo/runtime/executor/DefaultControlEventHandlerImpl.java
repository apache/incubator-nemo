package org.apache.nemo.runtime.executor;

import org.apache.nemo.common.RuntimeIdManager;
import org.apache.nemo.conf.JobConf;
import org.apache.nemo.runtime.common.comm.ControlMessage;
import org.apache.nemo.runtime.common.message.PersistentConnectionToMasterMap;
import org.apache.nemo.runtime.executor.common.ControlEventHandler;
import org.apache.nemo.runtime.executor.common.TaskExecutor;
import org.apache.nemo.runtime.executor.common.TaskHandlingEvent;
import org.apache.nemo.runtime.executor.common.controlmessages.TaskControlMessage;
import org.apache.nemo.runtime.executor.common.controlmessages.TaskStopSignalByMaster;
import org.apache.nemo.runtime.executor.common.datatransfer.InputPipeRegister;
import org.apache.nemo.runtime.executor.common.datatransfer.PipeManagerWorker;
import org.apache.reef.tang.annotations.Parameter;

import javax.inject.Inject;

import static org.apache.nemo.runtime.common.message.MessageEnvironment.SCALE_DECISION_MESSAGE_LISTENER_ID;

public final class DefaultControlEventHandlerImpl implements ControlEventHandler {

  private final String executorId;
  private final TaskExecutorMapWrapper taskExecutorMapWrapper;
  private final PipeManagerWorker pipeManagerWorker;
  private final PersistentConnectionToMasterMap toMaster;

  @Inject
  private DefaultControlEventHandlerImpl(
    @Parameter(JobConf.ExecutorId.class) final String executorId,
    final TaskExecutorMapWrapper taskExecutorMapWrapper,
    final PipeManagerWorker pipeManagerWorker,
    final PersistentConnectionToMasterMap toMaster) {
    this.executorId = executorId;
    this.taskExecutorMapWrapper = taskExecutorMapWrapper;
    this.pipeManagerWorker = pipeManagerWorker;
    this.toMaster = toMaster;
  }

  @Override
  public void handleControlEvent(TaskHandlingEvent event) {
    final TaskControlMessage control = (TaskControlMessage) event.getControl();

    switch (control.type) {
      case TASK_STOP_SIGNAL_BY_MASTER: {
        final TaskStopSignalByMaster msg = (TaskStopSignalByMaster) control.event;
        final TaskExecutor taskExecutor = taskExecutorMapWrapper.getTaskExecutor(msg.taskId);

        if (taskExecutor.isSource()) {
          if (!pipeManagerWorker.isOutputPipeStopped(taskExecutor.getId())) {
            // stop and remove task now
            // there is no pending event
            stopAndCheckpointTask(taskExecutor.getId());
          }
        } else {
          // Stop input pipe
          taskExecutor.getTask().getUpstreamTasks().entrySet().forEach(entry -> {
            pipeManagerWorker.sendStopSignalForInputPipes(entry.getValue(),
              entry.getKey().getId(), msg.taskId);
          });
        }
        break;
      }
      case PIPE_OUTPUT_STOP_SIGNAL_BY_DOWNSTREAM_TASK: {
        // do not send data any more
        final int pipeIndex = control.targetPipeIndex;
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
        pipeManagerWorker.startOutputPipe(control.targetPipeIndex, control.getTaskId());

        if (canTaskMoved(control.getTaskId())) {
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
    return pipeManagerWorker.getInputPipeState(taskId).equals(InputPipeRegister.InputPipeState.STOPPED)
      && !pipeManagerWorker.isOutputPipeStopped(taskId);
  }

  private void stopAndCheckpointTask(final String taskId) {
    // stop and remove task
    final TaskExecutor taskExecutor = taskExecutorMapWrapper.getTaskExecutor(taskId);
    taskExecutorMapWrapper.removeTask(taskExecutor);
    taskExecutor.checkpoint();

    toMaster.getMessageSender(SCALE_DECISION_MESSAGE_LISTENER_ID)
      .send(ControlMessage.Message.newBuilder()
        .setId(RuntimeIdManager.generateMessageId())
        .setListenerId(SCALE_DECISION_MESSAGE_LISTENER_ID)
          .setType(ControlMessage.MessageType.StopTaskDone)
          .setStopTaskDoneMsg(ControlMessage.StopTaskDoneMessage.newBuilder()
            .setExecutorId(executorId)
            .setTaskId(taskId)
            .build())
          .build());
  }
}
