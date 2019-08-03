package org.apache.nemo.runtime.executor.common.datatransfer;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufOutputStream;
import io.netty.buffer.PooledByteBufAllocator;
import org.apache.nemo.common.TaskLoc;

import java.io.*;

import static org.apache.nemo.runtime.executor.common.datatransfer.ByteTransferContextSetupMessage.MessageType.*;

public final class ByteTransferContextSetupMessage {

  public enum ByteTransferDataDirection {
    INITIATOR_SENDS_DATA,
    INITIATOR_RECEIVES_DATA
  }

  public enum MessageType {
    CONTROL,
    //RESTART,

    SIGNAL_FROM_CHILD_FOR_STOP_OUTPUT,
    ACK_FROM_PARENT_STOP_OUTPUT,
    SETTING_INPUT_CONTEXT, // setting in VM

    SIGNAL_FROM_CHILD_FOR_RESTART_OUTPUT,

    SIGNAL_FROM_PARENT_STOPPING_OUTPUT,
    ACK_FROM_CHILD_RECEIVE_PARENT_STOP_OUTPUT,
    SETTING_OUTPUT_CONTEXT, // setting in VM

    SIGNAL_FROM_PARENT_RESTARTING_OUTPUT,

    //STOP_INPUT_FOR_SCALEIN,
    //STOP_OUTPUT_FOR_SCALEIN,
    //ACK_FOR_STOP_OUTPUT,
    //ACK_FOR_STOP_INPUT,
    //RESUME_AFTER_SCALEOUT_VM,
    //RESUME_AFTER_SCALEIN_DOWNSTREAM_VM,
    //RESUME_AFTER_SCALEIN_UPSTREAM_VM,
  }

  private final String initiatorExecutorId;
  private final int transferIndex;
  private final ByteTransferDataDirection dataDirection;
  private final byte[] contextDescriptor;
  private final boolean isPipe;
  //private final String address;
  //private final String taskId;
  private final MessageType messageType;
  private final String relayServerAddress;
  private final int relayServerPort;
  private final TaskLoc location;
  private final String taskId;

  public ByteTransferContextSetupMessage(
    final String initiatorExecutorId,
    final int transferIndex,
    final ByteTransferDataDirection dataDirection,
    final byte[] contextDescriptor,
    final boolean isPipe) {
    this(initiatorExecutorId, transferIndex, dataDirection, contextDescriptor, isPipe, CONTROL, TaskLoc.VM, "");
  }

  public ByteTransferContextSetupMessage(
    final String initiatorExecutorId,
    final int transferIndex,
    final ByteTransferDataDirection dataDirection,
    final byte[] contextDescriptor,
    final boolean isPipe,
    final MessageType messageType,
    final TaskLoc location,
    final String taskId) {
    this(initiatorExecutorId, transferIndex, dataDirection, contextDescriptor, isPipe, messageType, location, taskId, "", 0);
  }

  public ByteTransferContextSetupMessage(
    final String initiatorExecutorId,
    final int transferIndex,
    final ByteTransferDataDirection dataDirection,
    final byte[] contextDescriptor,
    final boolean isPipe,
    final MessageType messageType,
    final TaskLoc location,
    final String taskId,
    final String relayServerAddress,
    final int relayServerPort) {
    this.initiatorExecutorId = initiatorExecutorId;
    this.transferIndex = transferIndex;
    this.dataDirection = dataDirection;
    this.contextDescriptor = contextDescriptor;
    this.isPipe = isPipe;
    this.location = location;
    this.messageType = messageType;
    this.relayServerAddress = relayServerAddress;
    this.relayServerPort = relayServerPort;
    this.taskId = taskId;
  }

  public String getTaskId() {
    return taskId;
  }

  public String getInitiatorExecutorId() {
    return initiatorExecutorId;
  }

  public ByteTransferDataDirection getDataDirection() {
    return dataDirection;
  }

  public int getTransferIndex() {
    return transferIndex;
  }

  public MessageType getMessageType () {
    return messageType;
  }

  public String getRelayServerAddress() {
    return relayServerAddress;
  }

  public int getRelayServerPort() {
    return relayServerPort;
  }

  public TaskLoc getLocation() {
    return location;
  }

  /*
  public String getMovedAddress() {
    return address;
  }

  public String getTaskId() {
    return taskId;
  }
  */

  public boolean getIsPipe() {
    return isPipe;
  }

  public byte[] getContextDescriptor() {
    return contextDescriptor;
  }

  public ByteBuf encode() {
     final ByteBuf byteBuf = PooledByteBufAllocator.DEFAULT.buffer();
    final ByteBufOutputStream bos = new ByteBufOutputStream(byteBuf);
    final DataOutputStream dos = new DataOutputStream(bos);
    try {
      dos.writeUTF(initiatorExecutorId);
      dos.writeInt(transferIndex);
      dos.writeInt(dataDirection.ordinal());
      dos.writeInt(contextDescriptor.length);
      dos.write(contextDescriptor);
      dos.writeBoolean(isPipe);
      dos.writeInt(messageType.ordinal());
      dos.writeInt(location.ordinal());
      dos.writeUTF(taskId);
      dos.writeUTF(relayServerAddress);
      dos.writeInt(relayServerPort);
      //dos.writeUTF(address); //dos.writeUTF(taskId);

      dos.close();
      bos.close();
    } catch (IOException e) {
      e.printStackTrace();
      throw new RuntimeException(e);
    }
    return byteBuf;
  }

  public static ByteTransferContextSetupMessage decode(final byte[] bytes,
                                                       final int offset,
                                                       final int length) {
    final ByteArrayInputStream bis = new ByteArrayInputStream(bytes, offset, length);
    final DataInputStream dis = new DataInputStream(bis);
    try {
      final String localExecutorId = dis.readUTF();
      final int transferIndex = dis.readInt();
      final ByteTransferDataDirection direction =
        ByteTransferDataDirection.values()[dis.readInt()];
      final int size = dis.readInt();
      final byte[] contextDescriptor = new byte[size];
      final int l = dis.read(contextDescriptor);
      if (l != size) {
        throw new RuntimeException("Invalid byte read: " + l + ", " + size);
      }
      final boolean isPipe = dis.readBoolean();
      final int ordinal = dis.readInt();
      final MessageType type = MessageType.values()[ordinal];
      final TaskLoc loc = TaskLoc.values()[dis.readInt()];
      final String taskId = dis.readUTF();
      final String relayServerAddress = dis.readUTF();
      final int relayServerPort = dis.readInt();

      return new ByteTransferContextSetupMessage(
        localExecutorId, transferIndex, direction, contextDescriptor,
        isPipe, type, loc, taskId, relayServerAddress, relayServerPort);

    } catch (IOException e) {
      e.printStackTrace();
      throw new RuntimeException(e);
    }
  }


  @Override
  public String toString() {
    return "InitExecutor: " + initiatorExecutorId + ", TransferIndex: " + transferIndex
      + ", " + "Direction: " + dataDirection + ", " + ", Type: "
      + messageType + "Addr: " + relayServerAddress + ", port: "+  relayServerPort + ", taskId: " + taskId;
  }
}
