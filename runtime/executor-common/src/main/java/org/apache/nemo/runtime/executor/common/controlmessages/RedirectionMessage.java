package org.apache.nemo.runtime.executor.common.controlmessages;

import java.io.*;

public final class RedirectionMessage {
  public final String originTaskId;
  public final String pairTaskId;
  public final String pairEdgeId;
  public final boolean checkpoint;

  public RedirectionMessage(final String originTaskId,
                            final String pairTaskId,
                            final String pairEdgeId,
                            final boolean checkpoint) {
    this.originTaskId = originTaskId;
    this.pairTaskId = pairTaskId;
    this.pairEdgeId = pairEdgeId;
    this.checkpoint = checkpoint;
  }

  public void encode(final OutputStream bos) {
    final DataOutputStream dos = new DataOutputStream(bos);
    try {
      dos.writeUTF(originTaskId);
      dos.writeUTF(pairTaskId);
      dos.writeUTF(pairEdgeId);
      dos.writeBoolean(checkpoint);
    } catch (final Exception e) {
      e.printStackTrace();
      throw new RuntimeException(e);
    }
  }

  public static RedirectionMessage decode(final InputStream is) {
    try {
      final DataInputStream dis = new DataInputStream(is);
      final String oid = dis.readUTF();
      final String pid = dis.readUTF();
      final String eid = dis.readUTF();
      final boolean checkpoint = dis.readBoolean();

      return new RedirectionMessage(
        oid, pid, eid, checkpoint);
    } catch (final Exception e) {
      e.printStackTrace();
      throw new RuntimeException(e);
    }
  }
}
