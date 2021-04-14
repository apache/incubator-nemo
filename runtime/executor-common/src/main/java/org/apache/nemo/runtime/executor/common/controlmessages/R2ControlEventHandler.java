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
import org.apache.nemo.runtime.executor.common.datatransfer.PipeManagerWorker;
import org.apache.nemo.runtime.executor.common.tasks.CRTaskExecutorImpl;
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
  }

  @Override
  public void handleControlEvent(TaskHandlingEvent event) {
    final TaskControlMessage control = (TaskControlMessage) event.getControl();
    if (evalConf.controlLogging) {
      LOG.info("Handling control event {} / {}", control.type, control);
    }

    switch (control.type) {
      case INVOKE_REDIRECTION_FOR_CR: {
        final TaskExecutor taskExecutor = taskExecutorMapWrapper.getTaskExecutor(control.getTaskId());
        final boolean init = (Boolean) control.event;

        if (!(taskExecutor.getTask().isVMTask() ||
          taskExecutor.getTask().isTransientTask())) {
          throw new RuntimeException("not supported for redirection " + taskExecutor.getId());
        }

        if (evalConf.controlLogging) {
          LOG.info("Send redirection message for {}->{} pairEdge {}",
            control.getTaskId(), taskExecutor.getTask().getPairTaskId(),
            taskExecutor.getTask().getPairEdgeId());
        }

        // stop pipe input of CR task
        taskExecutor.getTask().getUpstreamTasks().entrySet().forEach(entry -> {
          pipeManagerWorker.sendStopSignalForInputPipes(entry.getValue(),
            entry.getKey().getId(), control.getTaskId(),
            (triple) -> {
              return new TaskControlMessage(
                TaskControlMessage.TaskControlMessageType
                  .PIPE_OUTPUT_STOP_SIGNAL_BY_DOWNSTREAM_TASK_FOR_REROUTING,
                triple.getLeft(), // my output pipe index
                triple.getMiddle(), // my input pipe index
                triple.getRight(), // srct ask id
                new RedirectionMessage(
                  control.getTaskId(),
                  taskExecutor.getTask().getPairTaskId(),
                  taskExecutor.getTask().getPairEdgeId(),
                  init));
            });
        });
        break;
      }
      case PIPE_OUTPUT_STOP_SIGNAL_BY_DOWNSTREAM_TASK_FOR_REROUTING: {
        // should be handled by cr task
        final TaskExecutor taskExecutor =
          taskExecutorMapWrapper.getTaskExecutor(control.getTaskId());

        if (!taskExecutor.getTask().isCrTask()) {
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
            .PIPE_OUTPUT_STOP_ACK_FROM_UPSTREAM_TASK_FOR_REROUTING,
          message.init);

        // Here, we should stop output pipe
        pipeManagerWorker.stopOutputPipeForRouting(originIndex, control.getTaskId());

        if (evalConf.controlLogging) {
          LOG.info("Write control message for pipe output stop ack {}", key);
        }

        // redirection
        final CRTaskExecutorImpl crTaskExecutor = (CRTaskExecutorImpl) taskExecutor;
        crTaskExecutor.setRerouting(originTaskId, pairTaskId, pairEdgeId);
        break;
      }
      case PIPE_OUTPUT_STOP_ACK_FROM_UPSTREAM_TASK_FOR_REROUTING: {
        final TaskExecutor taskExecutor =
          taskExecutorMapWrapper.getTaskExecutor(control.getTaskId());

        if (!(taskExecutor.getTask().isVMTask() || taskExecutor.getTask().isTransientTask())) {
          throw new RuntimeException("Invalid task receive " +
            "PIPE_OUTPUT_STOP_ACK_FROM_UPSTREAM_TASK_FOR_REROUTING " + control.getTaskId());
        }

        if (evalConf.controlLogging) {
          LOG.info("Receive ACK for task {} pipe {}", control.getTaskId(), control.targetPipeIndex);
        }

        pipeManagerWorker.receiveAckInputStopSignal(control.getTaskId(), control.targetPipeIndex);

        final boolean init = (Boolean) control.event;

        if (pipeManagerWorker.isInputPipeStopped(control.getTaskId())) {
          taskOutputDoneAckCounter.put(control.getTaskId(),
            new AtomicInteger(taskOutgoingEdgeDoneAckCounter(taskExecutor.getTask())));

          final String srcTask = taskExecutor.getId();
          final int index = RuntimeIdManager.getIndexFromTaskId(srcTask);
          taskExecutor.getTask().getTaskOutgoingEdges()
            .forEach(outgoingEdge -> {
              if (outgoingEdge.getDataCommunicationPattern()
                .equals(CommunicationPatternProperty.Value.TransientOneToOne)
                || outgoingEdge.getDataCommunicationPattern()
                .equals(CommunicationPatternProperty.Value.OneToOne)) {

                final String dstTaskId =
                  RuntimeIdManager.generateTaskId(outgoingEdge.getDst().getId(), index, 0);
                pipeManagerWorker.writeControlMessage(srcTask, outgoingEdge.getId(), dstTaskId,
                  TaskControlMessage.TaskControlMessageType.TASK_OUTPUT_DONE,
                  null);
                if (evalConf.controlLogging) {
                  LOG.info("Send task output done signal from {} to {}", srcTask,
                    dstTaskId);
                }
              } else {
                final int parallelism = outgoingEdge.getSrcIRVertex()
                  .getPropertyValue(ParallelismProperty.class).get();

                for (int i = 0; i < parallelism; i++) {
                  final String dstTaskId =
                    RuntimeIdManager.generateTaskId(outgoingEdge.getDst().getId(), i, 0);
                  pipeManagerWorker.writeControlMessage(srcTask, outgoingEdge.getId(), dstTaskId,
                    TaskControlMessage.TaskControlMessageType.TASK_OUTPUT_DONE,
                    null);
                  if (evalConf.controlLogging) {
                    LOG.info("Send task output done signal from {} to {}", srcTask,
                      dstTaskId);
                  }
                }
              }
            });

          if (evalConf.controlLogging) {
            LOG.info("End of Receive ACK for task {} pipe {}", control.getTaskId());
          }

          taskInitMap.put(taskExecutor.getId(), init);


          taskExecutorMapWrapper.setTaskExecutorState(taskExecutor,
            TaskExecutorMapWrapper.TaskExecutorState.DEACTIVATED);
        }
        break;
      }
      case TASK_OUTPUT_DONE: {
        final TaskExecutor taskExecutor =
          taskExecutorMapWrapper.getTaskExecutor(control.getTaskId());

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
                .TASK_OUTPUT_DONE_ACK,
              triple.getLeft(), // my output pipe index
              triple.getMiddle(), // my input pipe index
              triple.getRight(),  // srct ask id
              null);
          });


        /* disable because we will use TASK_INPUT_START signal

        // Set input pipe to STOPPED for watermark handling
        final TaskExecutor taskExecutor = taskExecutorMapWrapper.getTaskExecutor(control.getTaskId());
        if (taskExecutor instanceof CRTaskExecutorImpl) {
          final CRTaskExecutorImpl crTaskExecutor = (CRTaskExecutorImpl) taskExecutor;
          final Triple<String, String, String> triple =
            pipeIndexMapWorker.getKey(control.remoteInputPipeIndex);
          crTaskExecutor.stopInputPipeIndex(triple);
        }
        */

        break;
      }
      case TASK_OUTPUT_DONE_ACK: {
         final TaskExecutor taskExecutor =
          taskExecutorMapWrapper.getTaskExecutor(control.getTaskId());

         LOG.info("Receive task output done ack {}, counter: {}", control.getTaskId(),
           taskOutputDoneAckCounter);

         final int cnt = taskOutputDoneAckCounter.get(control.getTaskId())
           .decrementAndGet();

        if (cnt == 0) {
          LOG.info("Receive all task output done ack {}", control.getTaskId());
          taskOutputDoneAckCounter.remove(control.getTaskId());

          final boolean init = taskInitMap.remove(control.getTaskId());

          final String pairTaskId = taskExecutor.getTask().getPairTaskId();

          if (!init) {
            taskExecutor.checkpoint(false, pairTaskId);
            // Send signal to master
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
            // Send signal to master
            toMaster.getMessageSender(RUNTIME_MASTER_MESSAGE_LISTENER_ID)
              .send(ControlMessage.Message.newBuilder()
                .setId(RuntimeIdManager.generateMessageId())
                .setListenerId(RUNTIME_MASTER_MESSAGE_LISTENER_ID.ordinal())
                .setType(ControlMessage.MessageType.InitSignal)
                .setStopTaskDoneMsg(ControlMessage.StopTaskDoneMessage.newBuilder()
                  .setExecutorId(executorId)
                  .setTaskId(control.getTaskId())
                  .build())
                .build());
          }
        }
        break;
      }
      case INIT_SIGNAL: {
        if (evalConf.controlLogging) {
          LOG.info("Init signal at {}", control.getTaskId());
        }

        final TaskExecutor taskExecutor =
          taskExecutorMapWrapper.getTaskExecutor(control.getTaskId());

        // send pipe init to output pipe
        taskExecutor.getTask().getDownstreamTasks().entrySet().forEach(entry -> {
          entry.getValue().forEach(dstTaskId -> {
            pipeManagerWorker.writeControlMessage(
              control.getTaskId(),
              entry.getKey().getId(),
              dstTaskId,
              TaskControlMessage.TaskControlMessageType.TASK_INPUT_START,
              null);
          });
        });
        break;
      }
      case GET_STATE_SIGNAL: {
        final TaskExecutor taskExecutor =
          taskExecutorMapWrapper.getTaskExecutor(control.getTaskId());


        if (evalConf.controlLogging) {
          LOG.info("Get checkpointed state in {}", control.getTaskId());
        }

        taskExecutor.restore();
        taskExecutorMapWrapper.setTaskExecutorState(taskExecutor,
          TaskExecutorMapWrapper.TaskExecutorState.RUNNING);

        // send pipe init to output pipe
        taskExecutor.getTask().getDownstreamTasks().entrySet().forEach(entry -> {
          entry.getValue().forEach(dstTaskId -> {
            pipeManagerWorker.writeControlMessage(
              control.getTaskId(),
              entry.getKey().getId(),
              dstTaskId,
              TaskControlMessage.TaskControlMessageType.TASK_INPUT_START,
              null);
          });
        });

        // Send signal to CR input pipes for rerouting
        // Here, reset the input of pairTask to running
        taskExecutor.getTask().getUpstreamTasks().entrySet().forEach(entry -> {
          pipeManagerWorker.sendSignalForInputPipes(entry.getValue(),
            entry.getKey().getId(), control.getTaskId(),
            (triple) -> {
              return new TaskControlMessage(
                TaskControlMessage.TaskControlMessageType
                  .STATE_MIGRATION_DONE,
                triple.getLeft(), // my output pipe index
                triple.getMiddle(), // my input pipe index
                triple.getRight(),  // srct ask id
                null);
            });
        });
        break;
      }
      case TASK_INPUT_START: {
        LOG.info("Receive task input start pipe {} running ", control.targetPipeIndex);

        final TaskExecutor taskExecutor = taskExecutorMapWrapper.getTaskExecutor(control.getTaskId());
        if (taskExecutor instanceof CRTaskExecutorImpl) {
          final CRTaskExecutorImpl crTaskExecutor = (CRTaskExecutorImpl) taskExecutor;
          final Triple<String, String, String> triple =
            pipeIndexMapWorker.getKey(control.remoteInputPipeIndex);
          crTaskExecutor.startInputPipeIndex(triple);
        }
        break;
      }
      case STATE_MIGRATION_DONE: {
        final long st = System.currentTimeMillis();
        LOG.info("State migration done receive at {} / {}", control.getTaskId(), control.targetPipeIndex);
        pipeManagerWorker.startOutputPipe(control.targetPipeIndex, control.getTaskId());
        final long et = System.currentTimeMillis();
        LOG.info("STAGE_MIGRATION output pipe flush done at {} / {} / {}", control.getTaskId(), control.targetPipeIndex,
          et - st);
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
