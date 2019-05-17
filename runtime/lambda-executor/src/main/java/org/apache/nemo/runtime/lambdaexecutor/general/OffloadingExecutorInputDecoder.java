package org.apache.nemo.runtime.lambdaexecutor.general;

import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.io.UnboundedSource;
import org.apache.nemo.compiler.frontend.beam.transform.GBKFinalState;
import org.apache.nemo.compiler.frontend.beam.transform.coders.GBKFinalStateCoder;
import org.apache.nemo.offloading.common.OffloadingDecoder;
import org.apache.nemo.runtime.executor.common.OffloadingExecutorEventType;
import org.apache.nemo.runtime.lambdaexecutor.downstream.TaskEndEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;

public final class OffloadingExecutorInputDecoder implements OffloadingDecoder<Object> {
  private static final Logger LOG = LoggerFactory.getLogger(OffloadingExecutorInputDecoder.class.getName());

  private final Coder<UnboundedSource.CheckpointMark> checkpointMarkCoder;
  private final Coder<GBKFinalState> stateCoder;

  public OffloadingExecutorInputDecoder(
    final Coder<UnboundedSource.CheckpointMark> checkpointMarkCoder,
    final Coder<GBKFinalState> stateCoder) {
    this.checkpointMarkCoder = checkpointMarkCoder;
    this.stateCoder = stateCoder;
  }

  @Override
  public Object decode(InputStream inputStream) throws IOException {
    //final byte[] bb = new byte[172480];
    //inputStream.read(bb);


    //System.out.println("--------BYTEARRAY-------");
    //System.out.println(Arrays.toString(bb));


    //final ByteArrayInputStream bis = new ByteArrayInputStream(bb);
    final DataInputStream dis = new DataInputStream(inputStream);
    final OffloadingExecutorEventType.EventType et =  OffloadingExecutorEventType.EventType.values()[dis.readInt()];

    switch (et) {
      case TASK_START: {
        return OffloadingTask.decode(inputStream, checkpointMarkCoder, stateCoder);
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
