package org.apache.nemo.runtime.executor.common.controlmessages;

import org.apache.commons.lang3.tuple.Triple;
import org.apache.nemo.common.RuntimeIdManager;
import org.apache.nemo.common.Task;
import org.apache.nemo.common.ir.edge.RuntimeEdge;
import org.apache.nemo.conf.EvalConf;
import org.apache.nemo.conf.JobConf;
import org.apache.nemo.offloading.common.TaskHandlingEvent;
import org.apache.nemo.runtime.common.comm.ControlMessage;
import org.apache.nemo.runtime.executor.common.ControlEventHandler;
import org.apache.nemo.runtime.executor.common.PipeIndexMapWorker;
import org.apache.nemo.runtime.executor.common.TaskExecutorMapWrapper;
import org.apache.nemo.runtime.executor.common.TaskExecutorUtil;
import org.apache.nemo.runtime.executor.common.datatransfer.PipeManagerWorker;
import org.apache.nemo.runtime.executor.common.tasks.CRTaskExecutor;
import org.apache.nemo.runtime.executor.common.tasks.PartialTaskExecutorImpl;
import org.apache.nemo.runtime.executor.common.tasks.ReroutingState;
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

import static org.apache.nemo.runtime.executor.common.controlmessages.TaskControlMessage.TaskControlMessageType.*;
import static org.apache.nemo.runtime.message.MessageEnvironment.ListenerType.RUNTIME_MASTER_MESSAGE_LISTENER_ID;


public final class R2ControlEventHandler implements ControlEventHandler {
  private static final Logger LOG = LoggerFactory.getLogger(R2ControlEventHandler.class.getName());

  private final String executorId;
  private final TaskExecutorMapWrapper taskExecutorMapWrapper;
  private final PipeManagerWorker pipeManagerWorker;
  private final PersistentConnectionToMasterMap toMaster;
  private final EvalConf evalConf;
  private final PipeIndexMapWorker pipeIndexMapWorker;
  private final Map<String, AtomicInteger> taskOutputDoneAckCounter;
  private final Map<String, Boolean> taskInitMap;
  private final Map<String, AtomicInteger> taskInputDoneAckCounter;

  @Inject
  private R2ControlEventHandler(
    @Parameter(JobConf.ExecutorId.class) final String executorId,
    final PipeIndexMapWorker pipeIndexMapWorker,
    final TaskExecutorMapWrapper taskExecutorMapWrapper,
    final PipeManagerWorker pipeManagerWorker,
    final EvalConf evalConf,
    final PersistentConnectionToMasterMap toMaster) {
    this.executorId = executorId;
    this.taskExecutorMapWrapper = taskExecutorMapWrapper;
    this.pipeManagerWorker = pipeManagerWorker;
    this.toMaster = toMaster;
    this.evalConf = evalConf;
    this.pipeIndexMapWorker = pipeIndexMapWorker;
    this.taskOutputDoneAckCounter = new ConcurrentHashMap<>();
    this.taskInitMap = new ConcurrentHashMap<>();
    this.taskInputDoneAckCounter = new ConcurrentHashMap<>();
  }

  /**
   * A ------> B -------> D
   * --------> C -------->
   * (1): Stop A----->B
   * (2): Checkpoint B and move state to C
   * (3): Start A---->C
   * (4): Stop B----->D
   * (5): Start C----->D
   * @param event
   */
  @Override
  public void handleControlEvent(TaskHandlingEvent event) {
    final TaskControlMessage control = (TaskControlMessage) event.getControl();

    switch (control.type) {
      // (1)
      case R2_INVOKE_REDIRECTION_FOR_CR_BY_MASTER: {
        final TaskExecutor taskExecutor = taskExecutorMapWrapper.getTaskExecutor(control.getTaskId());
        final boolean checkpoint = (Boolean) control.event;

        if (!(taskExecutor.getTask().isVMTask() ||
          taskExecutor.getTask().isTransientTask())) {
          throw new RuntimeException("not supported for redirection " + taskExecutor.getId());
        }

        if (evalConf.controlLogging) {
          LOG.info("Send redirection message for {}->{} pairEdge {}",
            control.getTaskId(), taskExecutor.getTask().getPairTaskId(),
            taskExecutor.getTask().getPairEdgeId());
        }

        final int cnt = TaskExecutorUtil.taskIncomingEdgeDoneAckCounter(taskExecutor.getTask());
        taskInputDoneAckCounter.put(taskExecutor.getId(), new AtomicInteger(cnt));

        // stop pipe input of CR task
        taskExecutor.getTask().getUpstreamTasks().entrySet().forEach(entry -> {
          pipeManagerWorker.sendSignalForInputPipes(entry.getValue(),
            entry.getKey().getId(), control.getTaskId(),
            (triple) -> {
              return new TaskControlMessage(
                TaskControlMessage.TaskControlMessageType
                  .R2_PIPE_OUTPUT_STOP_SIGNAL_FROM_TASK_TO_CR,
                triple.getLeft(), // my output pipe index
                triple.getMiddle(), // my input pipe index
                triple.getRight(), // srct ask id
                new RedirectionMessage(
                  control.getTaskId(),
                  taskExecutor.getTask().getPairTaskId(),
                  taskExecutor.getTask().getPairEdgeId(),
                  checkpoint));
            });
        });
        break;
      }
      // (1): stop input pipe
      case R2_PIPE_OUTPUT_STOP_SIGNAL_FROM_TASK_TO_CR: {
        // should be handled by cr task
        final TaskExecutor taskExecutor =
          taskExecutorMapWrapper.getTaskExecutor(control.getTaskId());

        if (!(taskExecutor instanceof CRTaskExecutor)) {
          throw new RuntimeException("Non CR TASK receives rerouting stop signal "
            + taskExecutor.getId() + ", " + taskExecutor.getTask().getTaskType());
        }

        final int originIndex = control.targetPipeIndex;
        final RedirectionMessage message = (RedirectionMessage) control.event;

        final Triple<String, String, String> key = pipeIndexMapWorker.getKey(originIndex);
        final String originTaskId = message.originTaskId;
        final String pairTaskId = message.pairTaskId;
        final String pairEdgeId = message.pairEdgeId;

        // final int redirectIndex = pipeIndexMapWorker.getPipeIndex(key.getLeft(), pairEdgeId, pairTaskId);

        if (evalConf.controlLogging) {
          LOG.info("Stopping upstream task pipe and redirection from {}->{} " +
              "pairEdge {} in {}, origin: {}, index {}", key.getRight(), pairTaskId, pairEdgeId, control.targetTaskId,
            key, originIndex);
        }

        // send ACK signal
        pipeManagerWorker.writeControlMessage(
          key.getLeft(), key.getMiddle(), key.getRight(),
          TaskControlMessage.TaskControlMessageType
            .R2_ACK_PIPE_OUTPUT_STOP_FROM_CR_TO_TASK,
          message.checkpoint);

        // Here, we should stop output pipe
        pipeManagerWorker.stopOutputPipeForRouting(originIndex, control.getTaskId());

        // We also stop the pair output pipe
        final int pairIndex = pipeIndexMapWorker.getPipeIndex(control.getTaskId(), pairEdgeId, pairTaskId);
        pipeManagerWorker.stopOutputPipeForRouting(pairIndex, control.getTaskId());

        // redirection
        final CRTaskExecutor crTaskExecutor = (CRTaskExecutor) taskExecutor;
        crTaskExecutor.setRerouting(originTaskId, pairTaskId, pairEdgeId,
          ReroutingState.DATA_WATERMARK_BOTH);

        if (evalConf.controlLogging) {
          LOG.info("Write control message for pipe output stop ack {}", key);
        }
        break;
      }
      // (1): stop input pipe
      case R2_ACK_PIPE_OUTPUT_STOP_FROM_CR_TO_TASK: {
        final TaskExecutor taskExecutor =
          taskExecutorMapWrapper.getTaskExecutor(control.getTaskId());

        if (!(taskExecutor.getTask().isVMTask()
          || taskExecutor.getTask().isTransientTask())) {
          throw new RuntimeException("Invalid task receive " +
            "R2_ACK_PIPE_OUTPUT_STOP_FROM_CR_TO_TASK " + control.getTaskId());
        }

        if (evalConf.controlLogging) {
          LOG.info("Receive ACK for task {} pipe {}", control.getTaskId(), control.targetPipeIndex);
        }

        final boolean checkpoint = (Boolean) control.event;
        // (2): send state
        final String pairTaskId = taskExecutor.getTask().getPairTaskId();

        // (3): close output
        if (taskInputDoneAckCounter.get(taskExecutor.getId()).decrementAndGet() == 0) {
          taskInputDoneAckCounter.remove(taskExecutor.getId());

          final int cnt = TaskExecutorUtil
              .taskOutgoingEdgeDoneAckCounter(taskExecutor.getTask());

          if (evalConf.controlLogging) {
            LOG.info("End of Receive ACK for task {} pipe {}", control.getTaskId());
          }

          final Task task = taskExecutor.getTask();

          // And we should send all of the pending
          if (cnt > 0) {

            // Checkpoint
            if (checkpoint) {
              taskExecutor.checkpoint(false, pairTaskId);
            } else {
              taskInitMap.put(control.getTaskId(), true);
            }

            LOG.info("Send task output done from upstream signal in {}", control.getTaskId());

            taskOutputDoneAckCounter.put(control.getTaskId(), new AtomicInteger(cnt));
            TaskExecutorUtil.sendOutputDoneMessage(task, pipeManagerWorker,
              R2_TASK_OUTPUT_DONE_FROM_UP_TO_DOWN);
            taskExecutorMapWrapper.setTaskExecutorState(taskExecutor,
              TaskExecutorMapWrapper.TaskExecutorState.DEACTIVATED);

            task.getDownstreamTasks().forEach((edge, val) -> {
              final String edgeId = edge.getId();
              val.forEach(dstTask -> {
                final int targetIndex = pipeIndexMapWorker.getPipeIndex(task.getTaskId(), edgeId, dstTask);
                pipeManagerWorker.stopOutputPipeForRouting(targetIndex, control.getTaskId());
              });
            });
          } else {
            if (checkpoint) {
              taskExecutor.checkpoint(false, pairTaskId);
              // Send signal to the pair task
              toMaster.getMessageSender(RUNTIME_MASTER_MESSAGE_LISTENER_ID)
                .send(ControlMessage.Message.newBuilder()
                  .setId(RuntimeIdManager.generateMessageId())
                  .setListenerId(RUNTIME_MASTER_MESSAGE_LISTENER_ID.ordinal())
                  .setType(ControlMessage.MessageType.GetStateSignal)
                  .setStopTaskDoneMsg(ControlMessage.StopTaskDoneMessage.newBuilder()
                    .setExecutorId(executorId)
                    .setTaskId(control.getTaskId())
                    .build())
                  .build());
            } else {
              // This is only at the start of job
              // Lambda task will be stopped without checkpointing
              toMaster.getMessageSender(RUNTIME_MASTER_MESSAGE_LISTENER_ID)
                .send(ControlMessage.Message.newBuilder()
                  .setId(RuntimeIdManager.generateMessageId())
                  .setListenerId(RUNTIME_MASTER_MESSAGE_LISTENER_ID.ordinal())
                  .setType(ControlMessage.MessageType.R2Init)
                  .setStopTaskDoneMsg(ControlMessage.StopTaskDoneMessage.newBuilder()
                    .setExecutorId(executorId)
                    .setTaskId(control.getTaskId())
                    .build())
                  .build());
            }
          }
        }
        break;
      }
      case R2_TASK_OUTPUT_DONE_FROM_UP_TO_DOWN: {
        final TaskExecutor taskExecutor =
          taskExecutorMapWrapper.getTaskExecutor(control.getTaskId());

        final Triple<String, String, String> key = pipeIndexMapWorker.getKey(control.remoteInputPipeIndex);

        if (evalConf.controlLogging) {
          LOG.info("Task output done signal received in {} for index {} / key {}", control.getTaskId(),
            control.targetPipeIndex, key);
        }

        //final CRTaskExecutor crTaskExecutor = (CRTaskExecutor) taskExecutor;

        final Triple<String, String, String> t =
          pipeIndexMapWorker.getKey(control.remoteInputPipeIndex);

        if (evalConf.controlLogging) {
          LOG.info("Stop input pipe of task {}, {}", taskExecutor.getId(), t);
        }
        // crTaskExecutor.stopInputPipeIndex(t);

        // Send ack
        pipeManagerWorker.sendSignalForInputPipes(Collections.singletonList(key.getLeft()),
          key.getMiddle(),
          control.getTaskId(),
          (triple) -> {
            return new TaskControlMessage(
              TaskControlMessage.TaskControlMessageType
                .R2_TASK_OUTPUT_DONE_ACK_FROM_DOWN_TO_UP,
              triple.getLeft(), // my output pipe index
              triple.getMiddle(), // my input pipe index
              triple.getRight(),  // srct ask id
              null);
          });
        break;
      }
      case R2_TASK_OUTPUT_DONE_ACK_FROM_DOWN_TO_UP: {
         final TaskExecutor taskExecutor =
          taskExecutorMapWrapper.getTaskExecutor(control.getTaskId());

         if (evalConf.controlLogging) {
           LOG.info("Receive task output done ack {}, counter: {}", control.getTaskId(),
             taskOutputDoneAckCounter);
         }

         final int cnt = taskOutputDoneAckCounter.get(control.getTaskId())
           .decrementAndGet();

         pipeManagerWorker.stopOutputPipeForRouting(control.targetPipeIndex, control.getTaskId());

        if (cnt == 0) {
          // (5): start pair task output pipe
          if (evalConf.controlLogging) {
            LOG.info("Receive all task output done ack {}", control.getTaskId());
            taskOutputDoneAckCounter.remove(control.getTaskId());
          }

          if (taskInitMap.containsKey(control.getTaskId())) {
            taskInitMap.remove(control.getTaskId());

            // Send signal to master
            toMaster.getMessageSender(RUNTIME_MASTER_MESSAGE_LISTENER_ID)
              .send(ControlMessage.Message.newBuilder()
                .setId(RuntimeIdManager.generateMessageId())
                .setListenerId(RUNTIME_MASTER_MESSAGE_LISTENER_ID.ordinal())
                .setType(ControlMessage.MessageType.R2Init)
                .setStopTaskDoneMsg(ControlMessage.StopTaskDoneMessage.newBuilder()
                  .setExecutorId(executorId)
                  .setTaskId(control.getTaskId())
                  .build())
                .build());
          } else {
            // Send signal to master for the pair task to start its output
            // This is also a signal that this task rerouting is done
            toMaster.getMessageSender(RUNTIME_MASTER_MESSAGE_LISTENER_ID)
              .send(ControlMessage.Message.newBuilder()
                .setId(RuntimeIdManager.generateMessageId())
                .setListenerId(RUNTIME_MASTER_MESSAGE_LISTENER_ID.ordinal())
                .setType(ControlMessage.MessageType.GetStateSignal)
                .setStopTaskDoneMsg(ControlMessage.StopTaskDoneMessage.newBuilder()
                  .setExecutorId(executorId)
                  .setTaskId(control.getTaskId())
                  .build())
                .build());
          }
        }
        break;
      }
      case R2_GET_STATE_SIGNAL_BY_PAIR: {
        final TaskExecutor taskExecutor =
          taskExecutorMapWrapper.getTaskExecutor(control.getTaskId());

        if (evalConf.controlLogging) {
          LOG.info("Get checkpointed state in {}", control.getTaskId());
        }
        taskExecutor.restore();
      }
      case R2_INIT: {
        final TaskExecutor taskExecutor =
          taskExecutorMapWrapper.getTaskExecutor(control.getTaskId());

        if (evalConf.controlLogging) {
          LOG.info("Get init in {}", control.getTaskId());
        }

        // Send signal to CR input pipes for rerouting
        // Here, reset the input of pairTask to running
        taskExecutor.getTask().getUpstreamTasks().entrySet().forEach(entry -> {
          pipeManagerWorker.sendSignalForInputPipes(entry.getValue(),
            entry.getKey().getId(), control.getTaskId(),
            (triple) -> {
              return new TaskControlMessage(
                TaskControlMessage.TaskControlMessageType
                  .R2_START_OUTPUT_FROM_DOWNSTREAM,
                triple.getLeft(), // my output pipe index
                triple.getMiddle(), // my input pipe index
                triple.getRight(),  // srct ask id
                null);
            });
        });

        // start output pipe
        taskExecutor.getTask().getDownstreamTasks().entrySet().forEach(entry -> {
          entry.getValue().forEach(dstTaskId -> {
            pipeManagerWorker.startOutputPipeForRerouting(
              control.getTaskId(),
              entry.getKey().getId(),
              dstTaskId,
              TaskControlMessage
                .TaskControlMessageType.R2_TASK_INPUT_START_FROM_UPSTREAM);
          });
        });

        taskExecutorMapWrapper.setTaskExecutorState(taskExecutor,
          TaskExecutorMapWrapper.TaskExecutorState.RUNNING);

        if (evalConf.controlLogging) {
          LOG.info("Init input pipe in {}", control.getTaskId());
        }
        break;
      }
      case R2_TASK_INPUT_START_FROM_UPSTREAM: {
        if (evalConf.controlLogging) {
          LOG.info("Receive task input start pipe {} running ", control.targetPipeIndex);
        }
        final TaskExecutor taskExecutor = taskExecutorMapWrapper.getTaskExecutor(control.getTaskId());
        if (taskExecutor instanceof CRTaskExecutor) {
          final CRTaskExecutor crTaskExecutor = (CRTaskExecutor) taskExecutor;
          final Triple<String, String, String> triple =
            pipeIndexMapWorker.getKey(control.remoteInputPipeIndex);
          crTaskExecutor.startAndStopInputPipeIndex(triple);
        }
        break;
      }
      case R2_START_OUTPUT_FROM_DOWNSTREAM: {
        final long st = System.currentTimeMillis();
        if (evalConf.controlLogging) {
          LOG.info("State migration done receive at {} / {}", control.getTaskId(), control.targetPipeIndex);
        }
        pipeManagerWorker.startOutputPipe(control.targetPipeIndex, control.getTaskId());
        final long et = System.currentTimeMillis();
        if (evalConf.controlLogging) {
          LOG.info("STAGE_MIGRATION output pipe flush done at {} / {} / {}", control.getTaskId(), control.targetPipeIndex,
            et - st);
        }
        break;
      }
      default:
        throw new RuntimeException("Invalid control message type " + control.type);
    }
  }
}
