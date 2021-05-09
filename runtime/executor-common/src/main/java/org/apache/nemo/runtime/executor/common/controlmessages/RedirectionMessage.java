package org.apache.nemo.runtime.executor.common.controlmessages;

import java.io.*;
import java.util.ArrayList;
import java.util.List;

public final class RedirectionMessage {
  public final String originTaskId;
  public final String pairTaskId;
  public final List<String> pairEdgeIds;
  public final boolean checkpoint;


  public RedirectionMessage(final String originTaskId,
                            final String pairTaskId,
                            final List<String> pairEdgeIds,
                            final boolean checkpoint) {
    this.originTaskId = originTaskId;
    this.pairTaskId = pairTaskId;
    this.pairEdgeIds = pairEdgeIds;
    this.checkpoint = checkpoint;
  }

  public void encode(final OutputStream bos) {
    final DataOutputStream dos = new DataOutputStream(bos);
    try {
      dos.writeUTF(originTaskId);
      dos.writeUTF(pairTaskId);
      dos.writeInt(pairEdgeIds.size());
      for (int i = 0; i < pairEdgeIds.size(); i++) {
        dos.writeUTF(pairEdgeIds.get(i));
      }
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
      final int s = dis.readInt();
      final List<String> pairEdges = new ArrayList<>(s);
      for (int i = 0; i < s; i++) {
        pairEdges.add(dis.readUTF());
      }
      final boolean checkpoint = dis.readBoolean();

      return new RedirectionMessage(
        oid, pid, pairEdges, checkpoint);
    } catch (final Exception e) {
      e.printStackTrace();
      throw new RuntimeException(e);
    }
  }
}
