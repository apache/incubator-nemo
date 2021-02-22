package org.apache.nemo.runtime.executor;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.ByteBufOutputStream;
import org.apache.nemo.common.Task;
import org.apache.nemo.runtime.executor.common.OffloadingPreparer;
import org.apache.nemo.runtime.executor.common.controlmessages.offloading.SendToOffloadingWorker;

import javax.inject.Inject;
import java.io.IOException;

import static org.apache.nemo.runtime.executor.common.OffloadingExecutorEventType.EventType.TASK_START;

public final class DefaultOffloadingPreparer implements OffloadingPreparer {

  private final PipeIndexMapWorker pipeIndexMapWorker;
  private final TaskExecutorMapWrapper taskExecutorMapWrapper;

  @Inject
  private DefaultOffloadingPreparer(final PipeIndexMapWorker pipeIndexMapWorker,
                                    final TaskExecutorMapWrapper taskExecutorMapWrapper) {
    this.pipeIndexMapWorker = pipeIndexMapWorker;
    this.taskExecutorMapWrapper = taskExecutorMapWrapper;
  }


  @Override
  public void prepare(String taskId, byte[] bytes) {
    final SendToOffloadingWorker taskSend =
      new SendToOffloadingWorker(taskId,
        bytes, pipeIndexMapWorker.getIndexMapForTask(taskId), true);
    final ByteBuf byteBuf = ByteBufAllocator.DEFAULT.buffer();
    final ByteBufOutputStream bos = new ByteBufOutputStream(byteBuf);

    try {
      bos.writeUTF(taskId);
      bos.writeInt(TASK_START.ordinal());
      taskSend.encode(bos);
      bos.close();
    } catch (IOException e) {
      e.printStackTrace();
      throw new RuntimeException(e);
    }

    taskExecutorMapWrapper.putTaskSerializedByte(taskId, byteBuf);
  }
}
