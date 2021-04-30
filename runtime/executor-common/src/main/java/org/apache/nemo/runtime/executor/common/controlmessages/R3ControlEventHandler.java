package org.apache.nemo.runtime.executor.common.controlmessages;

import org.apache.commons.lang3.tuple.Triple;
import org.apache.nemo.common.RuntimeIdManager;
import org.apache.nemo.common.Task;
import org.apache.nemo.common.ir.edge.RuntimeEdge;
import org.apache.nemo.common.ir.edge.executionproperty.CommunicationPatternProperty;
import org.apache.nemo.common.ir.vertex.executionproperty.ParallelismProperty;
import org.apache.nemo.conf.EvalConf;
import org.apache.nemo.conf.JobConf;
import org.apache.nemo.offloading.common.TaskHandlingEvent;
import org.apache.nemo.runtime.common.comm.ControlMessage;
import org.apache.nemo.runtime.executor.common.*;
import org.apache.nemo.runtime.executor.common.datatransfer.PipeManagerWorker;
import org.apache.nemo.runtime.executor.common.tasks.*;
import org.apache.nemo.runtime.message.PersistentConnectionToMasterMap;
import org.apache.reef.tang.annotations.Parameter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
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
  private final Map<String, AtomicInteger> taskOutputStopCounter;
  private final Map<String, Boolean> partialTaskOutputToBeStopped;
  private final Map<String, Boolean> partialTaskOutputAck;
  private final Map<String, Boolean> partialTaskStopRemaining;
  private final Map<String, Boolean> pairTaskWaitingAck;
  private final Map<String, AtomicInteger> taskInputStopCounter;
  private final PartialTaskDoneChecker partialTaskDoneChecker;

  @Inject
  private R3ControlEventHandler(
    @Parameter(JobConf.ExecutorId.class) final String executorId,
    final PipeIndexMapWorker pipeIndexMapWorker,
    final TaskExecutorMapWrapper taskExecutorMapWrapper,
    final PipeManagerWorker pipeManagerWorker,
    final EvalConf evalConf,
    final PartialTaskDoneChecker partialTaskDoneChecker,
    final PersistentConnectionToMasterMap toMaster) {
    this.executorId = executorId;
    this.taskExecutorMapWrapper = taskExecutorMapWrapper;
    this.pipeManagerWorker = pipeManagerWorker;
    this.toMaster = toMaster;
    this.evalConf = evalConf;
    this.pipeIndexMapWorker = pipeIndexMapWorker;
    this.partialTaskDoneChecker = partialTaskDoneChecker;
    this.partialTaskOutputToBeStopped = new ConcurrentHashMap<>();
    this.partialTaskOutputAck = new ConcurrentHashMap<>();
    this.partialTaskStopRemaining = new ConcurrentHashMap<>();
    this.taskOutputStopCounter = new ConcurrentHashMap<>();
    this.pairTaskWaitingAck = new ConcurrentHashMap<>();
    this.taskInputStopCounter = new ConcurrentHashMap<>();
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
        throw new RuntimeException("Not supported");
        /*
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
                  .R2_PIPE_OUTPUT_STOP_SIGNAL_FROM_TASK_TO_CR,
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
        */
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

        partialTaskDoneChecker.registerPartialDoneReadyTask(taskExecutor.getId());

        // PING PONG BTW PAIR TASK TO SYNC AND GUARANTEE PARTIAL OFFLOADING
        toMaster.getMessageSender(RUNTIME_MASTER_MESSAGE_LISTENER_ID)
          .send(ControlMessage.Message.newBuilder()
            .setId(RuntimeIdManager.generateMessageId())
            .setListenerId(RUNTIME_MASTER_MESSAGE_LISTENER_ID.ordinal())
            .setType(ControlMessage.MessageType.R3PairTaskInitiateProtocol)
            .setStopTaskDoneMsg(ControlMessage.StopTaskDoneMessage.newBuilder()
              .setExecutorId(executorId)
              .setTaskId(control.getTaskId())
              .build())
            .build());
        break;
      }
      case R3_PAIR_TASK_INITIATE_REROUTING_PROTOCOL: {
        final TaskExecutor taskExecutor = taskExecutorMapWrapper.getTaskExecutor(control.getTaskId());

        if (!(taskExecutor.getTask().isParitalCombine())) {
          throw new RuntimeException("Not supported for R3 redirection " + taskExecutor.getId());
        }

        if (partialTaskOutputToBeStopped.containsKey(taskExecutor.getId())
           && !partialTaskStopRemaining.containsKey(taskExecutor.getId())) {
          // This represents that taskExecutor is performing partial aggregation (waiting for key=0)
          // We stop this process by removing partialTaskOutputToBeStopped
          // and send ack to the pair task
          partialTaskOutputAck.remove(taskExecutor.getId());

          LOG.info("Task {} sending partial combine.. but stop this process", taskExecutor.getId());
          partialTaskOutputToBeStopped.remove(taskExecutor.getId());

          toMaster.getMessageSender(RUNTIME_MASTER_MESSAGE_LISTENER_ID)
            .send(ControlMessage.Message.newBuilder()
              .setId(RuntimeIdManager.generateMessageId())
              .setListenerId(RUNTIME_MASTER_MESSAGE_LISTENER_ID.ordinal())
              .setType(ControlMessage.MessageType.R3AckPairTaskInitiateProtocol)
              .setStopTaskDoneMsg(ControlMessage.StopTaskDoneMessage.newBuilder()
                .setExecutorId(executorId)
                .setTaskId(control.getTaskId())
                .build())
              .build());
        } else if (partialTaskStopRemaining.containsKey(taskExecutor.getId())) {
          // Waiting for the remaining process
          LOG.info("Waiting for the remaining process for partial combine", taskExecutor.getId());
          pairTaskWaitingAck.put(taskExecutor.getId(), true);
        } else {
          LOG.info("Sending rerouting protocol ack in {}", taskExecutor.getId());
          toMaster.getMessageSender(RUNTIME_MASTER_MESSAGE_LISTENER_ID)
            .send(ControlMessage.Message.newBuilder()
              .setId(RuntimeIdManager.generateMessageId())
              .setListenerId(RUNTIME_MASTER_MESSAGE_LISTENER_ID.ordinal())
              .setType(ControlMessage.MessageType.R3AckPairTaskInitiateProtocol)
              .setStopTaskDoneMsg(ControlMessage.StopTaskDoneMessage.newBuilder()
                .setExecutorId(executorId)
                .setTaskId(control.getTaskId())
                .build())
              .build());
        }
        break;
      }
      case R3_ACK_PAIR_TASK_INITIATE_REROUTING_PROTOCOL: {
        final TaskExecutor taskExecutor = taskExecutorMapWrapper.getTaskExecutor(control.getTaskId());

        if (!(taskExecutor.getTask().isParitalCombine())) {
          throw new RuntimeException("Not supported for R3 redirection " + taskExecutor.getId());
        }

        partialTaskOutputToBeStopped.put(taskExecutor.getId(), true);

        final int cnt = TaskExecutorUtil.taskIncomingEdgeDoneAckCounter(taskExecutor.getTask());
        // Stop data send from upstream
        taskInputStopCounter.put(taskExecutor.getId(), new AtomicInteger(cnt));

        taskExecutor.getTask().getUpstreamTasks().entrySet().forEach(entry -> {
          entry.getValue().forEach(srcTask -> {
            pipeManagerWorker.writeControlMessage(control.getTaskId(), entry.getKey().getId(), srcTask,
              R3_DATA_STOP_FROM_P_TO_CR,
              new RedirectionMessage(
                control.getTaskId(),
                taskExecutor.getTask().getPairTaskId(),
                taskExecutor.getTask().getPairEdgeId(),
                false));
          });
        });
        break;
      }
      case R3_DATA_WATERMARK_STOP_FROM_P_TO_CR:
      case R3_DATA_STOP_FROM_P_TO_CR: {
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
        final Triple<String, String, String> key = pipeIndexMapWorker.getKey(control.remoteInputPipeIndex);

        if (evalConf.controlLogging) {
          LOG.info("R3 data stop by downstream task {} for data rerouting to {} in {}," +
              "type {}, key {}",
            originTaskId, pairTaskId, control.getTaskId(), control.type, key);
        }

        if (control.type.equals(R3_DATA_STOP_FROM_P_TO_CR)) {
          // redirection only data
          final CRTaskExecutor crTaskExecutor = (CRTaskExecutor) taskExecutor;
          // Stop the output pipe of pair task
          // to pending the data until we send the input/output start signal to the pair task
          final int pairTaskIndex = pipeIndexMapWorker.getPipeIndex(control.getTaskId(),
            pairEdgeId, pairTaskId);

          pipeManagerWorker.stopOutputPipeForRouting(pairTaskIndex, control.getTaskId());
          crTaskExecutor.setRerouting(originTaskId, pairTaskId, pairEdgeId,
            ReroutingState.DATA_ONLY);

          // send ack
          pipeManagerWorker.writeControlMessage(key.getRight(), key.getMiddle(), key.getLeft(),
            R3_ACK_DATA_STOP_FROM_CR_TO_P,
            null);
        } else {
          // redirection data and watermark
          final CRTaskExecutor crTaskExecutor = (CRTaskExecutor) taskExecutor;
          crTaskExecutor.setRerouting(originTaskId, pairTaskId, pairEdgeId,
            ReroutingState.DATA_WATERMARK_BOTH);

          // send ack
          pipeManagerWorker.writeControlMessage(key.getRight(), key.getMiddle(), key.getLeft(),
            R3_ACK_DATA_WATERMARK_STOP_FROM_CR_TO_P,
            null);

          if (evalConf.controlLogging) {
            LOG.info("Write control message for pipe output stop ack {}", key);
          }
        }
        break;
      }
      case R3_ACK_DATA_STOP_FROM_CR_TO_P: {
        final TaskExecutor taskExecutor =
          taskExecutorMapWrapper.getTaskExecutor(control.getTaskId());

        if (evalConf.controlLogging) {
          LOG.info("R3 data stop ack from CR to p in {}", control.getTaskId());
        }

        if (!taskExecutor.getTask().isParitalCombine()) {
          throw new RuntimeException("Task is not partial combine " + control.getTaskId() + ", "
          + taskInputStopCounter);
        }

        final int cnt = taskInputStopCounter.get(control.getTaskId()).decrementAndGet();

        if (cnt == 0) {
          if (evalConf.controlLogging) {
            LOG.info("R3 pair input output start in {}", control.getTaskId());
          }

          taskInputStopCounter.remove(control.getTaskId());

          // output data done ack !!! (to guarantee there is no pending data in network)
          final int outCnt = TaskExecutorUtil.taskOutgoingEdgeDoneAckCounter(taskExecutor.getTask());
          if (outCnt == 0) {
            throw new RuntimeException("Partial cannot have zero outgoing edge " + taskExecutor.getId());
          }

          // Set pair task stopped false
          // This means that the partial task will emit partial result
          ((PartialTaskExecutorImpl) taskExecutor).setPairTaskStopped(false);

          taskOutputStopCounter.put(taskExecutor.getId(), new AtomicInteger(outCnt));
          taskExecutor.getTask().getDownstreamTasks().entrySet().forEach(entry -> {
            entry.getValue().forEach(dstTaskId -> {
              pipeManagerWorker.writeControlMessage(
                control.getTaskId(),
                entry.getKey().getId(),
                dstTaskId,
                R3_TASK_DATA_DONE_FROM_P_TO_M,
                control.getTaskId() + "/" +
                  taskExecutor.getTask().getPairTaskId());
            });
          });

        }
        break;
      }
      case R3_TASK_DATA_DONE_FROM_P_TO_M: {
        final TaskExecutor taskExecutor =
          taskExecutorMapWrapper.getTaskExecutor(control.getTaskId());

        if (!taskExecutor.getTask().isMerger()) {
          throw new RuntimeException("It is not merger " + control.getTaskId());
        }

        final String msg = (String) control.event;
        final String[] m = msg.split("/");
        final String originTask = m[0];
        final String pairTask = m[1];

        final Triple<String, String, String> key =
          pipeIndexMapWorker.getKey(control.remoteInputPipeIndex);

        final String pairEdgeId = taskExecutor.getTask().getTaskIncomingEdges()
          .stream().filter(edge -> !edge.getId().equals(key.getMiddle()))
          .findFirst().get().getId();

        if (evalConf.controlLogging) {
          LOG.info("Start input pipe of {}/{}/{} and send ack to {}",
            pairTask, pairEdgeId, control.getTaskId(),
            key);
        }

        // open the pair task input pipe (for watermark)
        ((CRTaskExecutor) taskExecutor).startInputPipeIndex(
          Triple.of(pairTask, pairEdgeId, control.getTaskId()));

        // set the merger will receieve partial result
        ((MergerTaskExecutor) taskExecutor).receivePartialFinal(false);

        // Send ack
        pipeManagerWorker.writeControlMessage(
          key.getRight(), key.getMiddle(), key.getLeft(),
          TaskControlMessage.TaskControlMessageType
            .R3_ACK_TASK_DATA_DONE_FROM_M_TO_P,
          null);
        break;
      }
      case R3_ACK_TASK_DATA_DONE_FROM_M_TO_P: {
        final TaskExecutor taskExecutor =
          taskExecutorMapWrapper.getTaskExecutor(control.getTaskId());

        LOG.info("Receive R3 ack task data done from m to p in {}", control.getTaskId());

        if (taskOutputStopCounter.get(control.getTaskId()).decrementAndGet() == 0) {
          taskOutputStopCounter.remove(control.getTaskId());
          // start input and output pipe of pair task !!
          // Send signal to master
          partialTaskOutputAck.put(control.getTaskId(), true);
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
                  .R3_START_OUTPUT_FROM_P_TO_CR,
                triple.getLeft(), // my output pipe index
                triple.getMiddle(), // my input pipe index
                triple.getRight(),  // srct ask id
                null);
            });
        });

        // (2) output pipe open
        taskExecutor.getTask().getDownstreamTasks().entrySet().forEach(entry -> {
          entry.getValue().forEach(dstTaskId -> {
            final int index = pipeIndexMapWorker.getPipeIndex(control.getTaskId(), entry.getKey().getId(), dstTaskId);
            pipeManagerWorker.startOutputPipe(index, control.getTaskId());
          });
        });

        break;
      }
      case R3_START_OUTPUT_FROM_P_TO_CR: {
        LOG.info("Start output at {} / {}", control.getTaskId(), control.targetPipeIndex);
        pipeManagerWorker.startOutputPipe(control.targetPipeIndex, control.getTaskId());
        break;
      }
      case R3_OPT_SIGNAL_FINAL_COMBINE_BY_PAIR: {
        final TaskExecutor taskExecutor =
          taskExecutorMapWrapper.getTaskExecutor(control.getTaskId());

        if (!taskExecutor.getTask().isParitalCombine()) {
          throw new RuntimeException("Non partial TASK receives pair done stop signal "
            + taskExecutor.getId() + ", " + taskExecutor.getTask().getTaskType());
        }

        if (partialTaskOutputToBeStopped.containsKey(control.getTaskId())) {
          LOG.info("This task rerouting protocol is initiated {} ... so skip set pair task stopped",
            control.getTaskId());
        } else {
          // Send downstream merger that it will send final result
          final RuntimeEdge mergerEdge = taskExecutor.getTask().getTaskOutgoingEdges().get(0);
          final String mergerId =
            TaskExecutorUtil.getDstTaskIds(control.getTaskId(), mergerEdge).get(0);

          LOG.info("Receive pair task output stopped in {}", control.getTaskId());
          ((PartialTaskExecutorImpl) taskExecutor).setPairTaskStopped(true);

          LOG.info("Send final result signal from {} to {}", control.getTaskId(), mergerId);
          pipeManagerWorker.writeControlMessage(control.getTaskId(), mergerEdge.getId(), mergerId,
            R3_OPT_SEND_FINAL_RESULT_FROM_P_TO_M, null);
        }

        break;
      }
      case R3_OPT_SEND_FINAL_RESULT_FROM_P_TO_M: {
        final TaskExecutor taskExecutor =
          taskExecutorMapWrapper.getTaskExecutor(control.getTaskId());

        if (!taskExecutor.getTask().isMerger()) {
          throw new RuntimeException("Non merger TASK receives final result signal "
            + taskExecutor.getId() + ", " + taskExecutor.getTask().getTaskType());
        }

        ((MergerTaskExecutor) taskExecutor).receivePartialFinal(true);
        break;
      }
      // Periodically check partial combine state
      case R3_TASK_STATE_CHECK: {
        final TaskExecutor taskExecutor = taskExecutorMapWrapper.getTaskExecutor(control.getTaskId());
        if (partialTaskOutputToBeStopped.containsKey(taskExecutor.getId())
          && partialTaskOutputAck.containsKey(taskExecutor.getId())) {
          if (taskExecutor.getNumKeys() == 0) {

            partialTaskOutputAck.remove(taskExecutor.getId());
            partialTaskOutputToBeStopped.remove(taskExecutor.getId());

            partialTaskStopRemaining.put(taskExecutor.getId(), true);

            // Deregister state checker
            partialTaskDoneChecker.deregisterPartialDoneTask(taskExecutor.getId());

            if (evalConf.controlLogging) {
              LOG.info("Try to stop data and watermark of input pipe of {}", taskExecutor.getId());
            }

            final int cnt = TaskExecutorUtil.taskIncomingEdgeDoneAckCounter(taskExecutor.getTask());
            taskInputStopCounter.put(taskExecutor.getId(), new AtomicInteger(cnt));
            // 1) stop input pipe
            taskExecutor.getTask().getUpstreamTasks().entrySet().forEach(entry -> {
              pipeManagerWorker.sendSignalForInputPipes(entry.getValue(),
                entry.getKey().getId(), control.getTaskId(),
                (triple) -> {
                  return new TaskControlMessage(
                    TaskControlMessage.TaskControlMessageType
                      .R3_DATA_WATERMARK_STOP_FROM_P_TO_CR,
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
      case R3_ACK_DATA_WATERMARK_STOP_FROM_CR_TO_P: {
        final TaskExecutor taskExecutor =
          taskExecutorMapWrapper.getTaskExecutor(control.getTaskId());

        if (!taskExecutor.getTask().isParitalCombine()) {
          throw new RuntimeException("Not partial task receive signal " + taskExecutor.getTask().getTaskId());
        }

        if (evalConf.controlLogging) {
          LOG.info("Receive data watermark stop ack for task {} pipe {}", control.getTaskId(), control.targetPipeIndex);
        }


        if (taskInputStopCounter.get(taskExecutor.getId()).decrementAndGet() == 0) {
          // close output
          taskInputStopCounter.remove(taskExecutor.getId());

          if (evalConf.controlLogging) {
            LOG.info("End of Receive ACK for task {} pipe {}", control.getTaskId());
          }

          // Send done message to the merger
          final Task task = taskExecutor.getTask();
          TaskExecutorUtil.sendOutputDoneMessage(task, pipeManagerWorker,
            R3_TASK_OUTPUT_DONE_FROM_P_TO_M);

          taskExecutorMapWrapper.setTaskExecutorState(taskExecutor,
            TaskExecutorMapWrapper.TaskExecutorState.DEACTIVATED);
        }
        break;
      }
      case R3_TASK_OUTPUT_DONE_FROM_P_TO_M: {
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

        // Send ack
        pipeManagerWorker.writeControlMessage(
          key.getRight(), key.getMiddle(), key.getLeft(),
          TaskControlMessage.TaskControlMessageType
            .R3_ACK_TASK_OUTPUT_DONE_ACK_FROM_M_TO_P,
          null);
        break;
      }
      case R3_ACK_TASK_OUTPUT_DONE_ACK_FROM_M_TO_P: {
        final TaskExecutor taskExecutor =
          taskExecutorMapWrapper.getTaskExecutor(control.getTaskId());

        if (!taskExecutor.getTask().isParitalCombine()) {
          throw new RuntimeException("Task is not partial combine " + control.getTaskId());
        }

        partialTaskStopRemaining.remove(control.getTaskId());

        // For optimization: send signal to partial that the pair task is stopped
        // This is also a signal that this task rerouting is done.
        toMaster.getMessageSender(RUNTIME_MASTER_MESSAGE_LISTENER_ID)
          .send(ControlMessage.Message.newBuilder()
            .setId(RuntimeIdManager.generateMessageId())
            .setListenerId(RUNTIME_MASTER_MESSAGE_LISTENER_ID.ordinal())
            .setType(ControlMessage.MessageType.R3OptSignalFinalCombine)
            .setStopTaskDoneMsg(ControlMessage.StopTaskDoneMessage.newBuilder()
              .setExecutorId(executorId)
              .setTaskId(control.getTaskId())
              .build())
            .build());

        if (pairTaskWaitingAck.containsKey(control.getTaskId())) {
          pairTaskWaitingAck.remove(control.getTaskId());

          toMaster.getMessageSender(RUNTIME_MASTER_MESSAGE_LISTENER_ID)
            .send(ControlMessage.Message.newBuilder()
              .setId(RuntimeIdManager.generateMessageId())
              .setListenerId(RUNTIME_MASTER_MESSAGE_LISTENER_ID.ordinal())
              .setType(ControlMessage.MessageType.R3AckPairTaskInitiateProtocol)
              .setStopTaskDoneMsg(ControlMessage.StopTaskDoneMessage.newBuilder()
                .setExecutorId(executorId)
                .setTaskId(control.getTaskId())
                .build())
              .build());
        }
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
