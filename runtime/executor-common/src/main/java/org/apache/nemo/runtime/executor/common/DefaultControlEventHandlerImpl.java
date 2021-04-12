package org.apache.nemo.runtime.executor.common;

import org.apache.nemo.common.RuntimeIdManager;
import org.apache.nemo.conf.EvalConf;
import org.apache.nemo.conf.JobConf;
import org.apache.nemo.runtime.common.comm.ControlMessage;
import org.apache.nemo.offloading.common.TaskHandlingEvent;
import org.apache.nemo.runtime.executor.common.controlmessages.TaskControlMessage;
import org.apache.nemo.runtime.executor.common.datatransfer.PipeManagerWorker;
import org.apache.nemo.runtime.executor.common.tasks.CRTaskExecutorImpl;
import org.apache.nemo.runtime.executor.common.tasks.DefaultTaskExecutorImpl;
import org.apache.nemo.runtime.executor.common.tasks.TaskExecutor;
import org.apache.nemo.runtime.executor.common.tasks.TransientTaskExecutorImpl;
import org.apache.nemo.runtime.message.PersistentConnectionToMasterMap;
import org.apache.reef.tang.annotations.Parameter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;

import static org.apache.nemo.runtime.executor.common.controlmessages.TaskControlMessage.TaskControlMessageType.PIPE_OUTPUT_STOP_SIGNAL_BY_DOWNSTREAM_TASK;
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
    if (evalConf.controlLogging) {
      LOG.info("Handling control event {} / {}", control.type, control);
    }

    switch (control.type) {
      case ROUTING_DATA_TO_LAMBDA_BY_MASTER: {
        final TaskExecutor taskExecutor = taskExecutorMapWrapper.getTaskExecutor(control.getTaskId());

        if (taskExecutor.isSource()) {
          throw new RuntimeException("not supported");
        } else {

          if (taskExecutor instanceof CRTaskExecutorImpl) {
            final CRTaskExecutorImpl crTask = (CRTaskExecutorImpl) taskExecutor;
            crTask.prepareRoutingToLambda();
          } else if (taskExecutor instanceof DefaultTaskExecutorImpl) {
            // we should stop input stream

          }

        }
        break;
      }
      case ROUTING_DATA_DONE_TO_LAMBDA_BY_MASTER: {
        final TaskExecutor taskExecutor = taskExecutorMapWrapper.getTaskExecutor(control.getTaskId());
        final CRTaskExecutorImpl crTask = (CRTaskExecutorImpl) taskExecutor;
        crTask.prepareRoutingDoneToLambda();
        break;
      }
      // From Transient -> CR
      case GET_STATE_DONE_SIGNAL_FROM_LAMBDA_TASK: {
        final TaskExecutor taskExecutor = taskExecutorMapWrapper.getTaskExecutor(control.getTaskId());
        final CRTaskExecutorImpl crTask = (CRTaskExecutorImpl) taskExecutor;
        crTask.startRoutingToLambda();
        break;
      }
      case STATE_CHECKPOINT_DONE_SIGNAL_FROM_LAMBDA_TASK: {
        final TaskExecutor taskExecutor = taskExecutorMapWrapper.getTaskExecutor(control.getTaskId());
        final CRTaskExecutorImpl crTask = (CRTaskExecutorImpl) taskExecutor;
        crTask.finishRoutingToLambda();
        break;
      }
      // From CR -> Transient
      case INIT_GET_STATE_SIGNAL_TO_LAMBDA_TASK: {
        final TaskExecutor taskExecutor = taskExecutorMapWrapper.getTaskExecutor(control.getTaskId());
        final TransientTaskExecutorImpl trTask = (TransientTaskExecutorImpl) taskExecutor;
        trTask.getStateFromRemote();
        break;
      }
      case INIT_STATE_CHECKPOINT_SIGNAL_TO_LAMBDA_TASK: {
        final TaskExecutor taskExecutor = taskExecutorMapWrapper.getTaskExecutor(control.getTaskId());
        final TransientTaskExecutorImpl trTask = (TransientTaskExecutorImpl) taskExecutor;
        trTask.stateMigrationToVM();
        break;
      }
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
      case TASK_SCHEDULED: {
        final String srcTaskId = control.getTaskId();
        final String dstTaskId = (String) control.event;
        pipeManagerWorker.taskScheduled(srcTaskId, dstTaskId);
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
    // flush pipes
    pipeManagerWorker.flush();

    // stop and remove task
    final TaskExecutor taskExecutor = taskExecutorMapWrapper.getTaskExecutor(taskId);
    taskExecutor.checkpoint(true);

    try {
      Thread.sleep(100);
    } catch (InterruptedException e) {
      e.printStackTrace();
    }

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
