package org.apache.nemo.runtime.executor;

import org.apache.nemo.runtime.executor.common.OffloadingPreparer;
import org.apache.nemo.runtime.executor.common.PipeIndexMapWorker;
import org.apache.nemo.runtime.executor.common.TaskExecutorMapWrapper;

import javax.inject.Inject;

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
    /*
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
    */
  }
}
