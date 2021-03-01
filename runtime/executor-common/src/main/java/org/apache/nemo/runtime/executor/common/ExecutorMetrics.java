package org.apache.nemo.runtime.executor.common;

import org.apache.nemo.common.Pair;

import javax.inject.Inject;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.Serializable;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

public final class ExecutorMetrics implements Serializable {

  public final Map<String, Pair<AtomicLong, AtomicLong>> taskInputProcessRateMap = new ConcurrentHashMap<>();
  public double load;
  public long processingRate;

  @Inject
  public ExecutorMetrics() {

  }

  public void encode(DataOutputStream dos) {
    try {
      dos.writeInt(taskInputProcessRateMap.size());
      for (final String key : taskInputProcessRateMap.keySet()) {
        final Pair<AtomicLong, AtomicLong> l = taskInputProcessRateMap.get(key);
        dos.writeUTF(key);
        dos.writeLong(l.left().get());
        dos.writeLong(l.right().get());
      }
    } catch (IOException e) {
      e.printStackTrace();
      throw new RuntimeException(e);
    }
  }

  public static ExecutorMetrics decode(DataInputStream dis) {
    try {

      final ExecutorMetrics em = new ExecutorMetrics();

      final int size = dis.readInt();
      final Map<String, Pair<AtomicLong, AtomicLong>> map = new ConcurrentHashMap<>();
      for (int i = 0; i < size; i++) {
        final String key = dis.readUTF();
        final Long v1 = dis.readLong();
        final Long v2 = dis.readLong();

        em.taskInputProcessRateMap.put(key, Pair.of(new AtomicLong(v1), new AtomicLong(v2)));
      }

      return em;
    } catch (final Exception e) {
      e.printStackTrace();
      throw new RuntimeException(e);
    }
  }

  @Override
  public String toString() {
    return "Rate: " + taskInputProcessRateMap.toString();
  }
}

