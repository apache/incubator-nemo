package org.apache.nemo.runtime.executor;

import org.apache.nemo.runtime.executor.common.ControlEventHandler;
import org.apache.nemo.runtime.executor.common.TaskExecutor;
import org.apache.nemo.runtime.executor.common.TaskHandlingEvent;
import org.apache.nemo.runtime.executor.common.controlmessages.TaskControlMessage;
import org.apache.nemo.runtime.executor.common.controlmessages.TaskStopSignalByMaster;
import org.apache.nemo.runtime.executor.common.datatransfer.InputPipeRegister;
import org.apache.nemo.runtime.executor.common.datatransfer.PipeManagerWorker;

import javax.inject.Inject;

public final class DefaultControlEventHandlerImpl implements ControlEventHandler {

  private final TaskExecutorMapWrapper taskExecutorMapWrapper;
  private final PipeManagerWorker pipeManagerWorker;

  @Inject
  private DefaultControlEventHandlerImpl(
    final TaskExecutorMapWrapper taskExecutorMapWrapper,
    final PipeManagerWorker pipeManagerWorker) {
    this.taskExecutorMapWrapper = taskExecutorMapWrapper;
    this.pipeManagerWorker = pipeManagerWorker;
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
            pipeManagerWorker.sendSignalForPipes(entry.getValue(),
              entry.getKey().getId(), msg.taskId, InputPipeRegister.Signal.INPUT_STOP);
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
      case PIPE_OUTPUT_RESTART_REQUEST_BY_DOWNSTREAM_TASK: {
        pipeManagerWorker.restartOutputPipe(control.targetPipeIndex, control.getTaskId());

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
  }
}
