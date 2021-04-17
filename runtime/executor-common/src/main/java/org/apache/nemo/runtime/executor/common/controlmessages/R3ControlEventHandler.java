package org.apache.nemo.runtime.executor.common.controlmessages;

import org.apache.commons.lang3.tuple.Triple;
import org.apache.nemo.common.RuntimeIdManager;
import org.apache.nemo.common.Task;
import org.apache.nemo.common.ir.edge.executionproperty.CommunicationPatternProperty;
import org.apache.nemo.common.ir.vertex.executionproperty.ParallelismProperty;
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


public final class R3ControlEventHandler implements ControlEventHandler {
  private static final Logger LOG = LoggerFactory.getLogger(R3ControlEventHandler.class.getName());

  private final String executorId;
  private final TaskExecutorMapWrapper taskExecutorMapWrapper;
  private final PipeManagerWorker pipeManagerWorker;
  private final PersistentConnectionToMasterMap toMaster;
  private final EvalConf evalConf;
  private final PipeIndexMapWorker pipeIndexMapWorker;
  private final Map<String, AtomicInteger> pairTaskInputStartCounter;
  private final Map<String, AtomicInteger> upstreamTaskStopCounter;
  private final Map<String, Boolean> partialTaskOutputToBeStopped;

  @Inject
  private R3ControlEventHandler(
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
    this.pairTaskInputStartCounter = new ConcurrentHashMap<>();
    this.partialTaskOutputToBeStopped = new ConcurrentHashMap<>();
    this.upstreamTaskStopCounter = new ConcurrentHashMap<>();
  }

  /**
   * A ----> B -----> Merger
   *   ----> C ----->
   *
   * When we want to merge B state to C
   *
   * (1): Send signal from B->M that C will start its output
   *  - waiting for ack... and (3)
   * (2): Send signal from B->A to send only data
   * (3): Send signal to C to receive data from A and send data to Merger
   * (4): Periodically check B state
   *  - If B's state becomes zero,
   *  (5): Send signal from B->Merger to stop B's output
   *
   *  After that,
   *  (6): the state of B is finally merged in the Merger.
   */
  @Override
  public void handleControlEvent(TaskHandlingEvent event) {
    final TaskControlMessage control = (TaskControlMessage) event.getControl();

    switch (control.type) {
      case R3_INIT: {
        // initial start
        // just use R2 path
        final TaskExecutor taskExecutor = taskExecutorMapWrapper.getTaskExecutor(control.getTaskId());
        // stop pipe input of CR task
        taskExecutor.getTask().getUpstreamTasks().entrySet().forEach(entry -> {
          pipeManagerWorker.sendStopSignalForInputPipes(entry.getValue(),
            entry.getKey().getId(), control.getTaskId(),
            (triple) -> {
              return new TaskControlMessage(
                TaskControlMessage.TaskControlMessageType
                  .R2_PIPE_OUTPUT_STOP_SIGNAL_BY_DOWNSTREAM_TASK_FOR_REROUTING,
                triple.getLeft(), // my output pipe index
                triple.getMiddle(), // my input pipe index
                triple.getRight(), // srct ask id
                new RedirectionMessage(
                  control.getTaskId(),
                  taskExecutor.getTask().getPairTaskId(),
                  taskExecutor.getTask().getPairEdgeId(),
                  false));
            });
        });
        break;
      }
      // (1)
      case R3_INVOKE_REDIRECTION_FOR_CR_BY_MASTER: {
        final TaskExecutor taskExecutor = taskExecutorMapWrapper.getTaskExecutor(control.getTaskId());

        if (!(taskExecutor.getTask().isParitalCombine())) {
          throw new RuntimeException("Not supported for R3 redirection " + taskExecutor.getId());
        }

        if (evalConf.controlLogging) {
          LOG.info("Send redirection message for {}->{} pairEdge {}",
            control.getTaskId(), taskExecutor.getTask().getPairTaskId(),
            taskExecutor.getTask().getPairEdgeId());
        }

        // Stop data send from upstream
        taskExecutor.getTask().getUpstreamTasks().entrySet().forEach(entry -> {
          entry.getValue().forEach(srcTask -> {
            pipeManagerWorker.writeControlMessage(control.getTaskId(), entry.getKey().getId(), srcTask,
              R3_DATA_STOP_BY_DOWNSTREMA_TASK,
              new RedirectionMessage(
                control.getTaskId(),
                taskExecutor.getTask().getPairTaskId(),
                taskExecutor.getTask().getPairEdgeId(),
                false));
          });
        });

        partialTaskOutputToBeStopped.put(taskExecutor.getId(), true);

        // send signal to downstream task that pair task will start its output
        pairTaskInputStartCounter.put(taskExecutor.getId(),
          new AtomicInteger(taskOutgoingEdgeDoneAckCounter(taskExecutor.getTask())));

        taskExecutor.getTask().getDownstreamTasks().entrySet().forEach(entry -> {
          entry.getValue().forEach(dstTaskId -> {
            pipeManagerWorker.writeControlMessage(
              control.getTaskId(),
              entry.getKey().getId(),
              dstTaskId,
              R3_OPEN_PAIR_TASK_INPUT_PIPE_SIGNAL_BY_UPSTREAM_TASK,
              control.getTaskId()+"/"+
                taskExecutor.getTask().getPairTaskId());
          });
        });

        break;
      }
      case R3_OPEN_PAIR_TASK_INPUT_PIPE_SIGNAL_BY_UPSTREAM_TASK: {
        final TaskExecutor taskExecutor = taskExecutorMapWrapper.getTaskExecutor(control.getTaskId());

        if (!taskExecutor.getTask().isMerger()) {
          throw new RuntimeException("Not merger task " + taskExecutor.getId());
        }

        final String msg = (String) control.event;
        final String[] m = msg.split("/");
        final String originTask = m[0];
        final String pairTask = m[1];


        final Triple<String, String, String> key =
          pipeIndexMapWorker.getKey(control.targetPipeIndex);


        final String pairEdgeId = taskExecutor.getTask().getTaskIncomingEdges()
          .stream().filter(edge -> !edge.getId().equals(key.getMiddle()))
          .findFirst().get().getId();

        if (evalConf.controlLogging) {
          LOG.info("Start input pipe of {}/{}/{} and send ack to {}",
            pairTask, pairEdgeId, control.getTaskId(),
            key);
        }

        ((CRTaskExecutor) taskExecutor).startInputPipeIndex(
          Triple.of(pairTask, pairEdgeId, control.getTaskId()));

        pipeManagerWorker.writeControlMessage(
            key.getRight(), key.getMiddle(), key.getLeft(),
            TaskControlMessage.TaskControlMessageType
              .R3_OPEN_PAIR_TASK_INPUT_PIPE_SIGNAL_ACK_BY_DOWNSTREAM_TASK,
            null);
        break;
      }
      case R3_OPEN_PAIR_TASK_INPUT_PIPE_SIGNAL_ACK_BY_DOWNSTREAM_TASK: {
        final TaskExecutor taskExecutor =
          taskExecutorMapWrapper.getTaskExecutor(control.getTaskId());

        final int cnt = pairTaskInputStartCounter.get(control.getTaskId()).decrementAndGet();

        if (cnt == 0) {
          // start input and output pipe of pair task !!
          // Send signal to master
          toMaster.getMessageSender(RUNTIME_MASTER_MESSAGE_LISTENER_ID)
            .send(ControlMessage.Message.newBuilder()
              .setId(RuntimeIdManager.generateMessageId())
              .setListenerId(RUNTIME_MASTER_MESSAGE_LISTENER_ID.ordinal())
              .setType(ControlMessage.MessageType.R3PairInputOutputStart)
              .setStopTaskDoneMsg(ControlMessage.StopTaskDoneMessage.newBuilder()
                .setExecutorId(executorId)
                .setTaskId(control.getTaskId())
                .build())
              .build());
        }
        break;
      }
      case R3_INPUT_OUTPUT_START_BY_PAIR: {
        final TaskExecutor taskExecutor =
          taskExecutorMapWrapper.getTaskExecutor(control.getTaskId());

        if (evalConf.controlLogging) {
          LOG.info("Input/output start in {}", control.getTaskId());
        }

        // (1) input start
        taskExecutor.getTask().getUpstreamTasks().entrySet().forEach(entry -> {
          pipeManagerWorker.sendSignalForInputPipes(entry.getValue(),
            entry.getKey().getId(), control.getTaskId(),
            (triple) -> {
              return new TaskControlMessage(
                TaskControlMessage.TaskControlMessageType
                  .R3_START_OUTPUT_FROM_DOWNSTREAM,
                triple.getLeft(), // my output pipe index
                triple.getMiddle(), // my input pipe index
                triple.getRight(),  // srct ask id
                null);
            });
        });

        // (2) output start
        taskExecutor.getTask().getDownstreamTasks().entrySet().forEach(entry -> {
          entry.getValue().forEach(dstTaskId -> {
            pipeManagerWorker.startOutputPipeForRerouting(
              control.getTaskId(),
              entry.getKey().getId(),
              dstTaskId,
              R3_TASK_INPUT_START_FROM_UPSTREAM);
          });
        });

        break;
      }
      case R3_START_OUTPUT_FROM_DOWNSTREAM: {
        LOG.info("Start output at {} / {}", control.getTaskId(), control.targetPipeIndex);
        pipeManagerWorker.startOutputPipe(control.targetPipeIndex, control.getTaskId());
        break;
      }
      case R3_TASK_INPUT_START_FROM_UPSTREAM: {
        LOG.info("Receive task input start pipe {} running {}", control.targetPipeIndex,
          control.getTaskId());
        final TaskExecutor taskExecutor = taskExecutorMapWrapper.getTaskExecutor(control.getTaskId());
        if (taskExecutor instanceof CRTaskExecutor) {
          final CRTaskExecutor crTaskExecutor = (CRTaskExecutor) taskExecutor;
          final Triple<String, String, String> triple =
            pipeIndexMapWorker.getKey(control.remoteInputPipeIndex);
          crTaskExecutor.startInputPipeIndex(triple);
        }
        break;
      }
      case R3_DATA_WATERMARK_STOP_BY_DOWNSTREMA_TASK:
      case R3_DATA_STOP_BY_DOWNSTREMA_TASK: {
        // should be handled by cr task
        final TaskExecutor taskExecutor =
          taskExecutorMapWrapper.getTaskExecutor(control.getTaskId());

        if (!taskExecutor.getTask().isCrTask()) {
          throw new RuntimeException("Non CR TASK receives rerouting stop signal "
            + taskExecutor.getId() + ", " + taskExecutor.getTask().getTaskType());
        }

        final RedirectionMessage message = (RedirectionMessage) control.event;

        final String originTaskId = message.originTaskId;
        final String pairTaskId = message.pairTaskId;
        final String pairEdgeId = message.pairEdgeId;

        if (evalConf.controlLogging) {
          LOG.info("R3 data stop by downstream task {} for data rerouting to {} in {}," +
              "type {}",
            originTaskId, pairTaskId, control.getTaskId(), control.type);
        }

        if (control.type.equals(R3_DATA_STOP_BY_DOWNSTREMA_TASK)) {
          // redirection only data
          final CRTaskExecutor crTaskExecutor = (CRTaskExecutor) taskExecutor;
          crTaskExecutor.setRerouting(originTaskId, pairTaskId, pairEdgeId,
            ReroutingState.DATA_ONLY);
        } else {
          // redirection data and watermark
          final CRTaskExecutor crTaskExecutor = (CRTaskExecutor) taskExecutor;
          crTaskExecutor.setRerouting(originTaskId, pairTaskId, pairEdgeId,
            ReroutingState.DATA_WATERMARK_BOTH);

          // send ack
          final Triple<String, String, String> key = pipeIndexMapWorker.getKey(control.targetPipeIndex);
          pipeManagerWorker.writeControlMessage(key.getLeft(), key.getMiddle(), key.getRight(),
            R3_DATA_WATERMARK_STOP_ACK_FROM_UPSTREAM_TASK_FOR_REROUTING,
            null);

          if (evalConf.controlLogging) {
            LOG.info("Write control message for pipe output stop ack {}", key);
          }
        }

        break;
      }
      // Periodically check partial combine state
      case R3_TASK_STATE_CHECK: {
        final TaskExecutor taskExecutor = taskExecutorMapWrapper.getTaskExecutor(control.getTaskId());
        if (partialTaskOutputToBeStopped.containsKey(taskExecutor.getId())
          && partialTaskOutputToBeStopped.get(taskExecutor.getId())) {
          if (taskExecutor.getNumKeys() == 0) {

            partialTaskOutputToBeStopped.remove(taskExecutor.getId());

            if (evalConf.controlLogging) {
              LOG.info("Try to stop data and watermark of input pipe of {}", taskExecutor.getId());
            }

            // 1) stop input pipe
            taskExecutor.getTask().getUpstreamTasks().entrySet().forEach(entry -> {
              pipeManagerWorker.sendStopSignalForInputPipes(entry.getValue(),
                entry.getKey().getId(), control.getTaskId(),
                (triple) -> {
                  return new TaskControlMessage(
                    TaskControlMessage.TaskControlMessageType
                      .R3_DATA_WATERMARK_STOP_BY_DOWNSTREMA_TASK,
                    triple.getLeft(), // my output pipe index
                    triple.getMiddle(), // my input pipe index
                    triple.getRight(), // srct ask id
                    new RedirectionMessage(
                      control.getTaskId(),
                      taskExecutor.getTask().getPairTaskId(),
                      taskExecutor.getTask().getPairEdgeId(),
                      false));
                });
            });
          }
        }
        break;
      }
      case R3_DATA_WATERMARK_STOP_ACK_FROM_UPSTREAM_TASK_FOR_REROUTING: {
        final TaskExecutor taskExecutor =
          taskExecutorMapWrapper.getTaskExecutor(control.getTaskId());

        if (evalConf.controlLogging) {
          LOG.info("Receive data watermark stop ack for task {} pipe {}", control.getTaskId(), control.targetPipeIndex);
        }

        pipeManagerWorker.receiveAckInputStopSignal(control.getTaskId(), control.targetPipeIndex);

        if (pipeManagerWorker.isInputPipeStopped(control.getTaskId())) {
          // close output

          if (evalConf.controlLogging) {
            LOG.info("End of Receive ACK for task {} pipe {}", control.getTaskId());
          }

          final Task task = taskExecutor.getTask();
          TaskExecutorUtil.sendOutputDoneMessage(task, pipeManagerWorker,
            R3_TASK_OUTPUT_DONE_FROM_UPSTREAM);

          taskExecutorMapWrapper.setTaskExecutorState(taskExecutor,
            TaskExecutorMapWrapper.TaskExecutorState.DEACTIVATED);
        }
        break;
      }
      case R3_TASK_OUTPUT_DONE_FROM_UPSTREAM: {
        final TaskExecutor taskExecutor =
          taskExecutorMapWrapper.getTaskExecutor(control.getTaskId());

        if (!taskExecutor.getTask().isMerger()) {
          throw new RuntimeException("Task is not merger " + control.getTaskId());
        }

        final Triple<String, String, String> key = pipeIndexMapWorker.getKey(control.remoteInputPipeIndex);

        if (evalConf.controlLogging) {
          LOG.info("Task output done signal received in {} for index {} / key {}", control.getTaskId(),
            control.targetPipeIndex, key);
        }

        // Set input pipe to STOPPED for watermark handling
        final CRTaskExecutor crTaskExecutor = (CRTaskExecutor) taskExecutor;
        final Triple<String, String, String> triple =
          pipeIndexMapWorker.getKey(control.remoteInputPipeIndex);
        crTaskExecutor.stopInputPipeIndex(triple);
        break;
      }
      default:
        throw new RuntimeException("Invalid control message type " + control.type);
    }
  }

  private int taskOutgoingEdgeDoneAckCounter(final Task task) {
    final AtomicInteger cnt = new AtomicInteger(0);

    task.getTaskOutgoingEdges()
      .forEach(outgoingEdge -> {
        if (outgoingEdge.getDataCommunicationPattern()
          .equals(CommunicationPatternProperty.Value.TransientOneToOne)
          || outgoingEdge.getDataCommunicationPattern()
          .equals(CommunicationPatternProperty.Value.OneToOne)) {
          cnt.getAndIncrement();
        } else {
          final int parallelism = outgoingEdge.getSrcIRVertex()
            .getPropertyValue(ParallelismProperty.class).get();
          cnt.getAndAdd(parallelism);
        }
      });

    return cnt.get();
  }
}
