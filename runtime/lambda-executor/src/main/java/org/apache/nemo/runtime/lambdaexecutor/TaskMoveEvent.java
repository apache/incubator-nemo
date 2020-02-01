package org.apache.nemo.runtime.lambdaexecutor;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufOutputStream;
import io.netty.buffer.PooledByteBufAllocator;
import org.apache.nemo.runtime.executor.common.OffloadingExecutorEventType;

import java.io.DataOutputStream;
import java.io.IOException;

public final class TaskMoveEvent {

  public final String taskId;

  public TaskMoveEvent(final String taskId) {
    this.taskId = taskId;
  }

  public ByteBuf encode() {
    final ByteBuf byteBuf = PooledByteBufAllocator.DEFAULT.buffer();
    final ByteBufOutputStream bos = new ByteBufOutputStream(byteBuf);
    final DataOutputStream dos = new DataOutputStream(bos);
    try {
      dos.writeInt(OffloadingExecutorEventType.EventType.TASK_MOVE.ordinal());
      dos.writeUTF(taskId);
    } catch (IOException e) {
      e.printStackTrace();
      throw new RuntimeException(e);
    }

    return byteBuf;
  }
}
