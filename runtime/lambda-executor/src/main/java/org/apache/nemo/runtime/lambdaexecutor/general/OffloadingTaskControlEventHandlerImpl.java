package org.apache.nemo.runtime.lambdaexecutor.general;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufOutputStream;
import io.netty.channel.Channel;
import org.apache.nemo.offloading.common.OffloadingEvent;
import org.apache.nemo.runtime.executor.common.ControlEventHandler;
import org.apache.nemo.runtime.executor.common.ExecutorThread;
import org.apache.nemo.runtime.executor.common.TaskExecutor;
import org.apache.nemo.offloading.common.TaskHandlingEvent;
import org.apache.nemo.runtime.executor.common.controlmessages.TaskControlMessage;
import org.apache.nemo.runtime.executor.common.datatransfer.DataFrameEncoder;
import org.apache.nemo.runtime.executor.common.datatransfer.OffloadingDataFrameEncoder;
import org.apache.nemo.runtime.executor.common.datatransfer.PipeManagerWorker;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Map;

import static org.apache.nemo.offloading.common.OffloadingEvent.Type.TASK_FINISH_DONE;


public final class OffloadingTaskControlEventHandlerImpl implements ControlEventHandler {
  private static final Logger LOG = LoggerFactory.getLogger(OffloadingTaskControlEventHandlerImpl.class.getName());

  private final String executorId;
  private final PipeManagerWorker pipeManagerWorker;
  private final Map<String, ExecutorThread> taskExecutorThreadMap;
  private final Map<String, TaskExecutor> taskExecutorMap;
  private final Channel executorDataChannel;
  private final Channel executorControlChannel;

  public OffloadingTaskControlEventHandlerImpl(
    final String executorId,
    final PipeManagerWorker pipeManagerWorker,
    final Map<String, ExecutorThread> taskExecutorThreadMap,
    final Map<String, TaskExecutor> taskExecutorMap,
    final Channel executorDataChannel,
    final Channel executorControlChannel) {
    this.executorId = executorId;
    this.pipeManagerWorker = pipeManagerWorker;
    this.taskExecutorThreadMap = taskExecutorThreadMap;
    this.taskExecutorMap = taskExecutorMap;
    this.executorDataChannel = executorDataChannel;
    this.executorControlChannel = executorControlChannel;
  }

  @Override
  public void handleControlEvent(TaskHandlingEvent event) {
    final TaskControlMessage control = (TaskControlMessage) event.getControl();
    switch (control.type) {
      case OFFLOAD_TASK_STOP: {
        final TaskExecutor taskExecutor = taskExecutorMap.get(control.getTaskId());
        stopAndCheckpointTask(taskExecutor.getId());

        final ByteBuf buf = executorDataChannel.alloc().ioBuffer();
        final ByteBufOutputStream bos = new ByteBufOutputStream(buf);
        try {
          bos.writeUTF(taskExecutor.getId());
          bos.close();

          LOG.info("Task finished {}", control.getTaskId());
          buf.retain();
          executorDataChannel.writeAndFlush(
            OffloadingDataFrameEncoder.DataFrame.newInstance(
              DataFrameEncoder.DataType.DEOFFLOAD_DONE, control.getTaskId()));

          executorControlChannel.writeAndFlush(new OffloadingEvent(TASK_FINISH_DONE, buf));

        } catch (IOException e) {
          e.printStackTrace();
          throw new RuntimeException(e);
        }

        break;
      }
      default:
        throw new RuntimeException("Invalid control message type " + control.type);
    }
  }

  private void stopAndCheckpointTask(final String taskId) {
    // stop and remove task
    final TaskExecutor taskExecutor = taskExecutorMap.remove(taskId);
    final ExecutorThread et = taskExecutorThreadMap.remove(taskId);
    et.deleteTask(taskExecutor);
    taskExecutor.checkpoint(false);
  }
}
