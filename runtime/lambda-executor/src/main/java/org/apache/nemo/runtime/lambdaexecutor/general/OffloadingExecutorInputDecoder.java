package org.apache.nemo.runtime.lambdaexecutor.general;

import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.io.UnboundedSource;
import org.apache.nemo.offloading.common.OffloadingDecoder;
import org.apache.nemo.runtime.executor.common.OffloadingExecutorEventType;
import org.apache.nemo.runtime.lambdaexecutor.downstream.TaskEndEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStream;

public final class OffloadingExecutorInputDecoder implements OffloadingDecoder<Object> {
  private static final Logger LOG = LoggerFactory.getLogger(OffloadingExecutorInputDecoder.class.getName());

  private final Coder<UnboundedSource.CheckpointMark> checkpointMarkCoder;

  public OffloadingExecutorInputDecoder(
    final Coder<UnboundedSource.CheckpointMark> checkpointMarkCoder) {
    this.checkpointMarkCoder = checkpointMarkCoder;
  }

  @Override
  public Object decode(InputStream inputStream) throws IOException {
    final DataInputStream dis = new DataInputStream(inputStream);
    final OffloadingExecutorEventType.EventType et =  OffloadingExecutorEventType.EventType.values()[dis.readInt()];

    switch (et) {
      case TASK_START: {
        return OffloadingTask.decode(inputStream, checkpointMarkCoder);
      }
      case TASK_END: {
        final String taskId = dis.readUTF();
        return new TaskEndEvent(taskId);
      }
      default:
        throw new RuntimeException("Not supported type: " + et);
    }
  }
}
