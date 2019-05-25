package org.apache.nemo.runtime.executor.common.datatransfer;

import java.io.*;

public class PipeTransferContextDescriptor {

  private final String runtimeEdgeId;
  private final long srcTaskIndex;
  private final long dstTaskIndex;
  private final long numPipe;
  private final ByteOutputContext.SendDataTo sendDataTo;

  public PipeTransferContextDescriptor(
    final String runtimeEdgeId,
    final long srcTaskIndex,
    final long dstTaskIndex,
    final long numPipe,
    final ByteOutputContext.SendDataTo sendDataTo) {
    this.runtimeEdgeId = runtimeEdgeId;
    this.srcTaskIndex = srcTaskIndex;
    this.dstTaskIndex = dstTaskIndex;
    this.numPipe = numPipe;
    this.sendDataTo = sendDataTo;
  }

  public String getRuntimeEdgeId() {
    return runtimeEdgeId;
  }

  public long getSrcTaskIndex() {
    return srcTaskIndex;
  }

  public long getDstTaskIndex() {
    return dstTaskIndex;
  }

  public long getNumPipe() {
    return numPipe;
  }

  public ByteOutputContext.SendDataTo getSendDataTo() {
    return sendDataTo;
  }

  public static PipeTransferContextDescriptor decode(final byte[] arr) {
    final ByteArrayInputStream bis = new ByteArrayInputStream(arr);
    final DataInputStream dis = new DataInputStream(bis);
    try {
      final String runtimeEdgeId = dis.readUTF();
      final long srcTaskIndex = dis.readLong();
      final long dstTaskIndex = dis.readLong();
      final long numPipe = dis.readLong();
      final ByteOutputContext.SendDataTo sdt = ByteOutputContext.SendDataTo.values()[dis.readInt()];
      return new PipeTransferContextDescriptor(runtimeEdgeId, srcTaskIndex, dstTaskIndex, numPipe, sdt);
    } catch (IOException e) {
      e.printStackTrace();
      throw new RuntimeException(e);
    }


  }

  public byte[] encode() {
    final ByteArrayOutputStream bos = new ByteArrayOutputStream();
    final DataOutputStream dos = new DataOutputStream(bos);
    try {
      dos.writeUTF(runtimeEdgeId);
      dos.writeLong(srcTaskIndex);
      dos.writeLong(dstTaskIndex);
      dos.writeLong(numPipe);
      dos.writeInt(sendDataTo.ordinal());
      dos.close();
      return bos.toByteArray();
    } catch (IOException e) {
      e.printStackTrace();
      throw new RuntimeException(e);
    }
  }
}
