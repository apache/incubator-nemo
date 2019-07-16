package org.apache.nemo.runtime.lambdaexecutor;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufOutputStream;
import io.netty.buffer.PooledByteBufAllocator;
import org.apache.nemo.common.NemoTriple;
import org.apache.nemo.common.coder.FSTSingleton;
import org.apache.nemo.runtime.executor.common.TaskLocationMap;
import org.nustaq.serialization.FSTConfiguration;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Map;

import static org.apache.nemo.runtime.executor.common.OffloadingExecutorEventType.EventType.TASK_READY;

public final class ReadyTask {

  public final String taskId;
  public final Map<NemoTriple<String, Integer, Boolean>, TaskLocationMap.LOC> taskLocationMap;

  public ReadyTask(final String taskId,
                   final Map<NemoTriple<String, Integer, Boolean>, TaskLocationMap.LOC> taskLocationMap) {
    this.taskId = taskId;
    this.taskLocationMap = taskLocationMap;
  }

  public ByteBuf encode() {
    final FSTConfiguration conf = FSTSingleton.getInstance();
    final ByteBuf byteBuf = PooledByteBufAllocator.DEFAULT.buffer();
    final ByteBufOutputStream bos = new ByteBufOutputStream(byteBuf);

    final DataOutputStream dos = new DataOutputStream(bos);

    try {
      dos.writeInt(TASK_READY.ordinal());
      dos.writeUTF(taskId);
      conf.encodeToStream(bos, taskLocationMap);

      dos.close();
      bos.close();
      return byteBuf;
    } catch (IOException e) {
      e.printStackTrace();
      throw new RuntimeException(e);
    }
  }

  public static ReadyTask decode(final InputStream inputStream) {

    final FSTConfiguration conf = FSTSingleton.getInstance();

    final DataInputStream dis = new DataInputStream(inputStream);
    try {
      final String taskId = dis.readUTF();
      final Map<NemoTriple<String, Integer, Boolean>, TaskLocationMap.LOC> taskLocationMap =
        (Map<NemoTriple<String, Integer, Boolean>, TaskLocationMap.LOC>) conf.decodeFromStream(inputStream);

      return new ReadyTask(taskId, taskLocationMap);
    } catch (Exception e) {
      e.printStackTrace();
      throw new RuntimeException(e);
    }
  }
}
