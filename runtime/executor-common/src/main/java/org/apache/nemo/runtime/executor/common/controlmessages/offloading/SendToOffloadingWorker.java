package org.apache.nemo.runtime.executor.common.controlmessages.offloading;

import org.apache.commons.lang3.SerializationUtils;
import org.apache.commons.lang3.tuple.Triple;

import java.io.*;
import java.util.Map;

public final class SendToOffloadingWorker {

  public final byte[] taskByte;
  public final Map<Triple<String, String, String>, Integer> indexMap;

  public SendToOffloadingWorker(final byte[] taskByte,
                                final Map<Triple<String, String, String>, Integer> indexMap) {
    this.taskByte = taskByte;
    this.indexMap = indexMap;
  }

  public void encode(OutputStream os) {
    final DataOutputStream dos = new DataOutputStream(os);
    try {
      dos.writeInt(taskByte.length);
      dos.write(taskByte);
      SerializationUtils.serialize(taskByte, os);
      SerializationUtils.serialize((Serializable) indexMap, os);
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
      final Map<Triple<String, String, String>, Integer> indexMap = SerializationUtils.deserialize(is);
      return new SendToOffloadingWorker(taskByte, indexMap);
    } catch (IOException e) {
      e.printStackTrace();
      throw new RuntimeException(e);
    }


  }
}
