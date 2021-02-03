package org.apache.nemo.runtime.executor.common.controlmessages.offloading;

import org.apache.commons.lang3.tuple.Triple;

import java.io.*;
import java.util.HashMap;
import java.util.Map;

import static org.apache.nemo.runtime.executor.common.OffloadingExecutorEventType.EventType.TASK_START;

public final class SendToOffloadingWorker {

  public final byte[] taskByte;
  public final Map<Triple<String, String, String>, Integer> indexMap;
  public final boolean offloaded;

  public SendToOffloadingWorker(final byte[] taskByte,
                                final Map<Triple<String, String, String>, Integer> indexMap,
                                final boolean offloaded) {
    this.taskByte = taskByte;
    this.indexMap = indexMap;
    this.offloaded = offloaded;
  }

  public void encode(OutputStream os) {
    final DataOutputStream dos = new DataOutputStream(os);
    try {
      dos.writeInt(taskByte.length);
      dos.write(taskByte);
      dos.writeInt(indexMap.size());
      indexMap.forEach((triple, val) -> {
        try {
          dos.writeUTF(triple.getLeft());
          dos.writeUTF(triple.getMiddle());
          dos.writeUTF(triple.getRight());
          dos.writeInt(val);
        } catch (IOException e) {
          e.printStackTrace();
          throw new RuntimeException(e);
        }
      });
      dos.writeBoolean(offloaded);
    } catch (IOException e) {
      e.printStackTrace();
      throw new RuntimeException(e);
    }

  }

  public static SendToOffloadingWorker decode(InputStream is) {
    final DataInputStream dis = new DataInputStream(is);
    try {
      final int len = dis.readInt();
      final byte[] taskByte = new byte[len];
      dis.read(taskByte);
      final int size = dis.readInt();
      final Map<Triple<String, String, String>, Integer> indexMap = new HashMap<>();
      for (int i = 0; i < size; i++) {
        final String src = dis.readUTF();
        final String edge = dis.readUTF();
        final String dst = dis.readUTF();
        final int index = dis.readInt();
        indexMap.put(Triple.of(src, edge, dst), index);
      }
      final boolean offloaded = dis.readBoolean();

      return new SendToOffloadingWorker(taskByte, indexMap, offloaded);
    } catch (IOException e) {
      e.printStackTrace();
      throw new RuntimeException(e);
    }


  }
}
