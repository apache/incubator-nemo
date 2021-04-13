package org.apache.nemo.runtime.executor.common.controlmessages;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufInputStream;
import io.netty.buffer.ByteBufOutputStream;
import org.apache.nemo.offloading.common.TaskHandlingEvent;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.OutputStream;
import java.util.Objects;

public final class TaskControlMessage implements TaskHandlingEvent {

  /*
   * For R2 reshaping:
   * CR -> Lambda
   *    -> VM
   *
   ****** TODO: Steps for moving Lambda task *****
   * (WE SHOULD STOP THE OUTPUT PIPE OF CR TASK AFTER LAMBDA TASK IS MOVED FROM VM->LAMBDA)
   *  - TODO:
   *
   * ****************************************************************************************
   * ****************************************************************************************
   * ****************************************************************************************
   *  STEPS for scheduling Lambda task to Lambda executor
   *  1) create lambda executor (resource type: LAMBDA)
   *  2) schedule lambda task (set resource affinity to LAMBDA)
   *  3) waiting for the scheduling of all tasks
   *  4) task done in lambda executor (Deactivate task)
   *  5) TODO: deactivate lamba executor
   *
   *  STEPS for activating and deactivating of Lambda tasks
   *  1) activate lambda executor
   *  2) redirection
   *  3) redirection done
   *  4) TODO: deactivate lambda executor
   *
   **************************************************************************************************
   **************************************************************************************************
   ****** Steps for rerouting from VM (Lambda) task to Lambda (VM) task ****
   * 1) from Master -> VM (Lambda) task: INVOKE_REDIRECTION_FOR_CR // done
   *  For VM->Lambda redirection
   *    - a) Driver: "redirection num stage_id"
   *    - b) Master: RoutingDataToLambda control message
   *    - c) Executor: receive RoutingDataToLambda control message and add INVOKE_REDI.. signal
   *  For Lambda->VM redirection
   *    - a) Driver: "redirection-done num stage_id"
   *    - b) Master: RoutingDataToLambdaDone control message (to the "pair" Lambda task)
   *    - c) Executor: receive RoutingDataToLambdaDone control message
   * ----------- (done) -----------
   * 2) VM (Lambda) task -> CR task: PIPE_OUTPUT_STOP_SIGNAL_BY_DOWNSTREAM_TASK_FOR_REROUTING
   *    - send: (originTaskId, pairTaskId, pairEdge)
   * 3) CR task: set redirection to Lambda/VM
   *    - a) add originTaksId, pairTaskId to rerouting table
   *    - b) redirect when data is sent to originTaskId;
   * 4) CR task -> VM/Lambda task: PIPE_OUTPUT_STOP_ACK_FROM_UPSTREAM_TASK_FOR_REROUTING // done
   *    - Here, input pipe state becomes STOPPED,
   *       we should change it to RUNNING when the data goes back to the task again
   * 5) VM/Lambda task: state checkpoint for Lambda/VM task // done
   *    - VM/Lambda task -> Master -> Lambda/VM task: signal GET_STATE_SIGNAL
   *    - Master: a) receives (VM/Lambda task id)
   *              b) find pair task id
   *              c) send GET_STATE_SIGNAL to the pair task id
   *    - After sending the message, the VM/Lambda task sends stop signal to output pipes
    *       -- (TASK_OUTPUT_DONE) signal; VM/Lambda task -> downstream tasks
    *       -- downstream tasks set their input pipe to STOPPED
   *        -- this is because of watermark handling of transient path
   * 6) Lambda/VM task (target task):
   *    - get checkpointed state (taskExecutor.restore())
   *    - Lambda/VM task -> CR task: STATE_MIGRATION_DONE (TODO)
   *       - a) send pipe init message to output pipes (TASK_INPUT_START) (TODO)
   *       - b) send STATE_MIGRATION_DONE to input pipes
   * 7) CR task:
   *    - receives STATE_MIGRATION_DONE and flush buffered data to Lambda/VM task
   **************************************************************************************************
   **************************************************************************************************
   * TODO: If Lambda instance is closed, how to handle lambad tasks? and redeploy?
   */

  public enum TaskControlMessageType {
    TASK_STOP_SIGNAL_BY_MASTER,
    INVOKE_REDIRECTION_FOR_CR,

    PIPE_OUTPUT_STOP_SIGNAL_BY_DOWNSTREAM_TASK,
    PIPE_OUTPUT_STOP_SIGNAL_BY_DOWNSTREAM_TASK_FOR_REROUTING,

    PIPE_OUTPUT_STOP_ACK_FROM_UPSTREAM_TASK,
    PIPE_OUTPUT_STOP_ACK_FROM_UPSTREAM_TASK_FOR_REROUTING,

    PIPE_INIT,
    OFFLOAD_CONTROL,
    REGISTER_EXECUTOR,
    DEACTIVATE_LAMBDA,
    BACKPRESSURE,
    BACKPRESSURE_RESTART,
    TASK_SCHEDULED,

    // normal and transient path
    GET_STATE_SIGNAL,
    STATE_MIGRATION_DONE,
    TASK_OUTPUT_DONE,
    TASK_INPUT_START,

    // For offloaded task
    OFFLOAD_TASK_STOP
  }

  public final TaskControlMessageType type;
  // remote의 input pipe index
  // 이 input pipe로 내가 input을 보내겠다!!
  public final int inputPipeIndex;

  // remote에서 input pipe index로 input 받은 뒤,
  // output pipe index로 뭐 주고 싶은 경우
  public final int targetPipeIndex;
  public final String targetTaskId;
  public final Object event;

  public TaskControlMessage(final TaskControlMessageType type,
                            final int inputPipeIndex,
                            final int targetPipeIndex,
                            final String targetTaskId,
                            final Object event) {
    this.type = type;
    this.inputPipeIndex = inputPipeIndex;
    this.targetPipeIndex = targetPipeIndex;
    this.targetTaskId = targetTaskId;
    this.event = event;
  }

  public boolean canShortcut() {
    switch (type) {
      case TASK_STOP_SIGNAL_BY_MASTER:
      case PIPE_INIT:
      case PIPE_OUTPUT_STOP_SIGNAL_BY_DOWNSTREAM_TASK: {
        return true;
      }
      default: {
        return false;
      }
    }
  }

  @Override
  public boolean isControlMessage() {
    return true;
  }

  @Override
  public boolean isOffloadingMessage() {
    return false;
  }

  @Override
  public ByteBuf getDataByteBuf() {
    throw new RuntimeException("This is control message");
  }

  @Override
  public String getEdgeId() {
    throw new RuntimeException("This is control message");
  }

  @Override
  public Object getData() {
    throw new RuntimeException("This is control message");
  }

  @Override
  public String getTaskId() {
    return targetTaskId;
  }

  @Override
  public int getInputPipeIndex() {
    return inputPipeIndex;
  }

  @Override
  public Object getControl() {
    return this;
  }

  public void encode(final ByteBufOutputStream bos) {
    try {
      bos.writeInt(type.ordinal());
      bos.writeInt(inputPipeIndex);
      bos.writeInt(targetPipeIndex);
      bos.writeUTF(targetTaskId);

      switch (type) {
        case PIPE_OUTPUT_STOP_SIGNAL_BY_DOWNSTREAM_TASK: {
          ((TaskStopSignalByDownstreamTask) event).encode(bos);
          break;
        }
        case REGISTER_EXECUTOR:{
          bos.writeUTF((String) event);
          break;
        }
        case PIPE_OUTPUT_STOP_SIGNAL_BY_DOWNSTREAM_TASK_FOR_REROUTING: {
          ((RedirectionMessage) event).encode(bos);
          break;
        }
        case PIPE_OUTPUT_STOP_ACK_FROM_UPSTREAM_TASK_FOR_REROUTING: {
          bos.writeBoolean((Boolean)event);
          break;
        }
        case PIPE_OUTPUT_STOP_ACK_FROM_UPSTREAM_TASK:
        case OFFLOAD_TASK_STOP:
        case PIPE_INIT:
        case DEACTIVATE_LAMBDA:
        default:
          break;
      }
    } catch (final Exception e) {
      e.printStackTrace();
      throw new RuntimeException(e);
    }
  }

  public static TaskControlMessage decode(final ByteBufInputStream bis) {
    try {
      final TaskControlMessageType type = TaskControlMessageType.values()[bis.readInt()];
      final int inputPipeIndex = bis.readInt();
      final int targetPipeIndex = bis.readInt();
      final String targetTaskId = bis.readUTF();

      TaskControlMessage msg = null;

      switch (type) {
        case PIPE_OUTPUT_STOP_SIGNAL_BY_DOWNSTREAM_TASK: {
          msg = new TaskControlMessage(type, inputPipeIndex, targetPipeIndex, targetTaskId,
            TaskStopSignalByDownstreamTask.decode(bis));
          break;
        }
        case PIPE_OUTPUT_STOP_SIGNAL_BY_DOWNSTREAM_TASK_FOR_REROUTING: {
          msg = new TaskControlMessage(type, inputPipeIndex, targetPipeIndex, targetTaskId,
            RedirectionMessage.decode(bis));
          break;
        }
        case PIPE_OUTPUT_STOP_ACK_FROM_UPSTREAM_TASK_FOR_REROUTING: {
          msg = new TaskControlMessage(type, inputPipeIndex, targetPipeIndex, targetTaskId,
            bis.readBoolean());
          break;
        }
        case REGISTER_EXECUTOR: {
          msg = new TaskControlMessage(type, inputPipeIndex,
            targetPipeIndex, targetTaskId, bis.readUTF());
          break;
        }
        case OFFLOAD_TASK_STOP:
        case PIPE_INIT:
        case DEACTIVATE_LAMBDA:
        case PIPE_OUTPUT_STOP_ACK_FROM_UPSTREAM_TASK:
        default:{
          msg = new TaskControlMessage(type, inputPipeIndex, targetPipeIndex, targetTaskId, null);
          break;
        }
      }
      return msg;
    } catch (final Exception e) {
      e.printStackTrace();
      throw new RuntimeException(e);
    }
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    TaskControlMessage that = (TaskControlMessage) o;
    return inputPipeIndex == that.inputPipeIndex &&
      type == that.type &&
      Objects.equals(targetTaskId, that.targetTaskId) &&
      Objects.equals(event, that.event);
  }

  @Override
  public int hashCode() {

    return Objects.hash(type, inputPipeIndex, targetTaskId, event);
  }
}
