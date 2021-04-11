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

  public enum TaskControlMessageType {
    TASK_STOP_SIGNAL_BY_MASTER,
    STATE_MIGRATION_SIGNAL_BY_MASTER,
    PIPE_OUTPUT_STOP_SIGNAL_BY_DOWNSTREAM_TASK,
    PIPE_OUTPUT_STOP_ACK_FROM_UPSTREAM_TASK,
    PIPE_INIT,
    OFFLOAD_CONTROL,
    REGISTER_EXECUTOR,
    DEACTIVATE_LAMBDA,
    BACKPRESSURE,
    BACKPRESSURE_RESTART,

    // For offloaded task
    OFFLOAD_TASK_STOP
  }

  public final TaskControlMessageType type;
  public final int inputPipeIndex;
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
        case OFFLOAD_TASK_STOP:
        case PIPE_INIT:
        case DEACTIVATE_LAMBDA:
        case PIPE_OUTPUT_STOP_ACK_FROM_UPSTREAM_TASK: {
          break;
        }
        default:
          throw new RuntimeException("Invalid control message type encoding " + type);
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
        case REGISTER_EXECUTOR: {
          msg = new TaskControlMessage(type, inputPipeIndex,
            targetPipeIndex, targetTaskId, bis.readUTF());
          break;
        }
        case OFFLOAD_TASK_STOP:
        case PIPE_INIT:
        case DEACTIVATE_LAMBDA:
        case PIPE_OUTPUT_STOP_ACK_FROM_UPSTREAM_TASK: {
          msg = new TaskControlMessage(type, inputPipeIndex, targetPipeIndex, targetTaskId, null);
          break;
        }
        default:
          throw new RuntimeException("invalid control message decoding " + type);
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
