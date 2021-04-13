package org.apache.nemo.runtime.executor.common.controlmessages;

import java.io.*;

public final class RedirectionMessage {
  public final String originTaskId;
  public final String pairTaskId;
  public final String pairEdgeId;
  public final boolean init;

  public RedirectionMessage(final String originTaskId,
                            final String pairTaskId,
                            final String pairEdgeId,
                            final boolean init) {
    this.originTaskId = originTaskId;
    this.pairTaskId = pairTaskId;
    this.pairEdgeId = pairEdgeId;
    this.init = init;
  }

  public void encode(final OutputStream bos) {
    final DataOutputStream dos = new DataOutputStream(bos);
    try {
      dos.writeUTF(originTaskId);
      dos.writeUTF(pairTaskId);
      dos.writeUTF(pairEdgeId);
      dos.writeBoolean(init);
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
      final boolean init = dis.readBoolean();

      return new RedirectionMessage(
        oid, pid, eid, init);
    } catch (final Exception e) {
      e.printStackTrace();
      throw new RuntimeException(e);
    }
  }
}
