package org.apache.nemo.runtime.executor.common.controlmessages;

import org.apache.commons.lang3.tuple.Triple;
import org.apache.nemo.common.RuntimeIdManager;
import org.apache.nemo.conf.EvalConf;
import org.apache.nemo.conf.JobConf;
import org.apache.nemo.runtime.common.comm.ControlMessage;
import org.apache.nemo.offloading.common.TaskHandlingEvent;
import org.apache.nemo.runtime.executor.common.ControlEventHandler;
import org.apache.nemo.runtime.executor.common.PipeIndexMapWorker;
import org.apache.nemo.runtime.executor.common.TaskExecutorMapWrapper;
import org.apache.nemo.runtime.executor.common.TaskExecutorUtil;
import org.apache.nemo.runtime.executor.common.datatransfer.PipeManagerWorker;
import org.apache.nemo.runtime.executor.common.tasks.TaskExecutor;
import org.apache.nemo.runtime.message.PersistentConnectionToMasterMap;
import org.apache.reef.tang.annotations.Parameter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;

import java.util.Collections;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

import static org.apache.nemo.runtime.executor.common.TaskExecutorUtil.taskOutgoingEdgeDoneAckCounter;
import static org.apache.nemo.runtime.executor.common.controlmessages.TaskControlMessage.TaskControlMessageType.TASK_OUTPUT_DONE_FROM_UPSTREAM;
import static org.apache.nemo.runtime.message.MessageEnvironment.ListenerType.RUNTIME_MASTER_MESSAGE_LISTENER_ID;


public final class DefaultControlEventHandlerImpl implements ControlEventHandler {
  private static final Logger LOG = LoggerFactory.getLogger(DefaultControlEventHandlerImpl.class.getName());

  private final String executorId;
  private final TaskExecutorMapWrapper taskExecutorMapWrapper;
  private final PipeManagerWorker pipeManagerWorker;
  private final PersistentConnectionToMasterMap toMaster;
  private final EvalConf evalConf;
  private final PipeIndexMapWorker pipeIndexMapWorker;
  private final R2ControlEventHandler r2ControlEventHandler;
  private final R3ControlEventHandler r3ControlEventHandler;
  private final TaskToBeStoppedMap taskToBeStopped;
  private final Map<String, AtomicInteger> taskOutputDoneAckCounter;

  @Inject
  private DefaultControlEventHandlerImpl(
    @Parameter(JobConf.ExecutorId.class) final String executorId,
    final PipeIndexMapWorker pipeIndexMapWorker,
    final TaskExecutorMapWrapper taskExecutorMapWrapper,
    final PipeManagerWorker pipeManagerWorker,
    final EvalConf evalConf,
    final R2ControlEventHandler r2ControlEventHandler,
    final R3ControlEventHandler r3ControlEventHandler,
    final TaskToBeStoppedMap taskToBeStoppedMap,
    final PersistentConnectionToMasterMap toMaster) {
    this.executorId = executorId;
    this.taskExecutorMapWrapper = taskExecutorMapWrapper;
    this.pipeManagerWorker = pipeManagerWorker;
    this.toMaster = toMaster;
    this.evalConf = evalConf;
    this.pipeIndexMapWorker = pipeIndexMapWorker;
    this.r2ControlEventHandler = r2ControlEventHandler;
    this.r3ControlEventHandler = r3ControlEventHandler;
    this.taskToBeStopped = taskToBeStoppedMap;
    this.taskOutputDoneAckCounter = new ConcurrentHashMap<>();
  }

  @Override
  public void handleControlEvent(TaskHandlingEvent event) {
    final TaskControlMessage control = (TaskControlMessage) event.getControl();

    switch (control.type) {
      // For optimization of R3 state merger
      case R3_PAIR_TASK_INITIATE_REROUTING_PROTOCOL:
      case R3_ACK_PAIR_TASK_INITIATE_REROUTING_PROTOCOL:
      case R3_TASK_OUTPUT_DONE_ACK_FROM_DOWNSTREAM:
      case R3_OPT_SEND_PARTIAL_RESULT_FROM_PARTIAL_TO_MERGER:
      case R3_OPT_SEND_FINAL_RESULT_FROM_PARTIAL_TO_MERGER:
      case R3_OPT_SIGNAL_FINAL_COMBINE_BY_PAIR:
      case R3_DATA_WATERMARK_STOP_BY_DOWNSTREMA_TASK:
      case R3_INVOKE_REDIRECTION_FOR_CR_BY_MASTER:
      case R3_DATA_WATERMARK_STOP_ACK_FROM_UPSTREAM_TASK_FOR_REROUTING:
      case R3_INIT:
      case R3_INPUT_OUTPUT_START_BY_PAIR:
      case R3_OPEN_PAIR_TASK_INPUT_PIPE_SIGNAL_ACK_BY_DOWNSTREAM_TASK:
      case R3_OPEN_PAIR_TASK_INPUT_PIPE_SIGNAL_BY_UPSTREAM_TASK:
      case R3_START_OUTPUT_FROM_DOWNSTREAM:
      case R3_TASK_INPUT_START_FROM_UPSTREAM:
      case R3_TASK_OUTPUT_DONE_FROM_UPSTREAM:
      case R3_TASK_STATE_CHECK:
      case R3_DATA_STOP_BY_DOWNSTREMA_TASK: {
        r3ControlEventHandler.handleControlEvent(event);
        break;
      }
      case R2_INIT:
      case R2_INVOKE_REDIRECTION_FOR_CR_BY_MASTER:
      case R2_PIPE_OUTPUT_STOP_ACK_FROM_UPSTREAM_TASK_FOR_REROUTING:
      case R2_PIPE_OUTPUT_STOP_SIGNAL_BY_DOWNSTREAM_TASK_FOR_REROUTING:
      case R2_TASK_OUTPUT_DONE_FROM_UPSTREAM:
      case R2_GET_STATE_SIGNAL_BY_PAIR:
      case R2_TASK_INPUT_START_FROM_UPSTREAM:
      case R2_TASK_OUTPUT_DONE_ACK_FROM_DOWNSTREAM:
      case R2_TASK_OUTPUT_START_BY_PAIR:
      case R2_INPUT_START_BY_PAIR:
      case R2_START_OUTPUT_FROM_DOWNSTREAM: {
        r2ControlEventHandler.handleControlEvent(event);
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
          taskToBeStopped.taskToBeStopped.put(taskExecutor.getId(), true);
          taskExecutor.getTask().getUpstreamTasks().entrySet().forEach(entry -> {
            pipeManagerWorker.sendStopSignalForInputPipes(entry.getValue(),
              entry.getKey().getId(), control.getTaskId(),
              (triple) -> {
                return new TaskControlMessage(
                  TaskControlMessage.TaskControlMessageType
                    .PIPE_OUTPUT_STOP_SIGNAL_BY_DOWNSTREAM_TASK,
                  triple.getLeft(), // my output pipe index
                  triple.getMiddle(), // my input pipe index
                  triple.getRight(), // srct ask id
                  new TaskStopSignalByDownstreamTask(control.getTaskId(),
                    entry.getKey().getId(), triple.getRight()));
              });
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

        if (taskToBeStopped.taskToBeStopped.containsKey(control.getTaskId()) &&
          pipeManagerWorker.isInputPipeStopped(control.getTaskId())) {
          final TaskExecutor taskExecutor =
            taskExecutorMapWrapper.getTaskExecutor(control.getTaskId());

          taskOutputDoneAckCounter.put(control.getTaskId(),
            new AtomicInteger(taskOutgoingEdgeDoneAckCounter(taskExecutor.getTask())));

          // stop output pipe
          TaskExecutorUtil.sendOutputDoneMessage(taskExecutor.getTask(), pipeManagerWorker,
            TASK_OUTPUT_DONE_FROM_UPSTREAM);
        }
        break;
      }
      case TASK_OUTPUT_DONE_FROM_UPSTREAM: {
        final Triple<String, String, String> key = pipeIndexMapWorker.getKey(control.remoteInputPipeIndex);

        if (evalConf.controlLogging) {
          LOG.info("Task output done signal received in {} for index {} / key {}", control.getTaskId(),
            control.targetPipeIndex, key);
        }

        // Send ack
        pipeManagerWorker.sendSignalForInputPipes(Collections.singletonList(key.getLeft()),
          key.getMiddle(),
          control.getTaskId(),
          (triple) -> {
            return new TaskControlMessage(
              TaskControlMessage.TaskControlMessageType
                .TASK_OUTPUT_DONE_ACK_FROM_DOWNSTREAM,
              triple.getLeft(), // my output pipe index
              triple.getMiddle(), // my input pipe index
              triple.getRight(),  // srct ask id
              null);
          });
        break;
      }
      case TASK_OUTPUT_DONE_ACK_FROM_DOWNSTREAM: {
        LOG.info("Receive task output done ack {}, counter: {}", control.getTaskId(),
          taskOutputDoneAckCounter);

        final int cnt = taskOutputDoneAckCounter.get(control.getTaskId())
          .decrementAndGet();

        pipeManagerWorker.stopOutputPipeForRouting(control.targetPipeIndex, control.getTaskId());

        if (cnt == 0) {
          // (5): start pair task output pipe
          LOG.info("Receive all task output done ack {}", control.getTaskId());
          taskOutputDoneAckCounter.remove(control.getTaskId());
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
          LOG.info("Pipe init message, key: {}, targetTask {}, in executor {}", pipeIndexMapWorker.getKey(control.targetPipeIndex), control.getTaskId(), executorId);
        }

        pipeManagerWorker.startOutputPipe(control.targetPipeIndex, control.getTaskId());
        /*
        if (canTaskMoved(control.getTaskId())) {
          if (evalConf.controlLogging) {
            LOG.info("Task can be moved {}, inputStateStopped {}, isOutputStoped: {}",
              control.getTaskId(), pipeManagerWorker.isInputPipeStopped(control.getTaskId()), pipeManagerWorker.isOutputPipeStopped(control.getTaskId())
            );
          }
          stopAndCheckpointTask(control.getTaskId());
        }
        */
        break;
      }
      default:
        throw new RuntimeException("Invalid control message type " + control.type);
    }
  }

  private boolean canTaskMoved(final String taskId) {
    // output stopped means that it is waiting for moving downstream task
    return taskToBeStopped.taskToBeStopped.containsKey(taskId) &&
      pipeManagerWorker.isInputPipeStopped(taskId)
      && !pipeManagerWorker.isOutputPipeStopped(taskId);
  }

  private void stopAndCheckpointTask(final String taskId) {
    // flush pipes
    pipeManagerWorker.flush();

    taskToBeStopped.taskToBeStopped.remove(taskId);

    // stop and remove task
    final TaskExecutor taskExecutor = taskExecutorMapWrapper.getTaskExecutor(taskId);
    taskExecutor.checkpoint(true, taskId);

    try {
      Thread.sleep(10);
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
