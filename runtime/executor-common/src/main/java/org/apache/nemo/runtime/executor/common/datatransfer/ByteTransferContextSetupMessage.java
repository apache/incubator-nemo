package org.apache.nemo.runtime.executor.common.datatransfer;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufOutputStream;
import io.netty.buffer.PooledByteBufAllocator;

import java.io.*;

public final class ByteTransferContextSetupMessage {

  public enum ByteTransferDataDirection {
    INITIATOR_SENDS_DATA,
    INITIATOR_RECEIVES_DATA
  }

  private final String initiatorExecutorId;
  private final int transferIndex;
  private final ByteTransferDataDirection dataDirection;
  private final byte[] contextDescriptor;
  private final boolean isPipe;
  private final boolean restart;

  public ByteTransferContextSetupMessage(
    final String initiatorExecutorId,
    final int transferIndex,
    final ByteTransferDataDirection dataDirection,
    final byte[] contextDescriptor,
    final boolean isPipe) {
    this(initiatorExecutorId, transferIndex, dataDirection, contextDescriptor, isPipe, false);
  }

  public ByteTransferContextSetupMessage(
    final String initiatorExecutorId,
    final int transferIndex,
    final ByteTransferDataDirection dataDirection,
    final byte[] contextDescriptor,
    final boolean isPipe,
    final boolean restart) {
    this.initiatorExecutorId = initiatorExecutorId;
    this.transferIndex = transferIndex;
    this.dataDirection = dataDirection;
    this.contextDescriptor = contextDescriptor;
    this.isPipe = isPipe;
    this.restart = restart;
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

  public boolean getIsRestart() {
    return restart;
  }

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
      dos.writeBoolean(restart);

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
      final boolean isRestart = dis.readBoolean();

      return new ByteTransferContextSetupMessage(
        localExecutorId, transferIndex, direction, contextDescriptor, isPipe, isRestart);
    } catch (IOException e) {
      e.printStackTrace();
      throw new RuntimeException(e);
    }
  }


  @Override
  public String toString() {
    return "InitExecutor: " + initiatorExecutorId + ", TransferIndex: " + transferIndex
      + ", " + "Direction: " + dataDirection + ", " + ", restart: " + restart;
  }
}
