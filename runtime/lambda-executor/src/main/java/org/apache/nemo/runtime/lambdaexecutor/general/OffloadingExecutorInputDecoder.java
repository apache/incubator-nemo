package org.apache.nemo.runtime.lambdaexecutor.general;

import io.netty.buffer.ByteBuf;
import org.apache.nemo.offloading.common.OffloadingDecoder;
import org.apache.nemo.runtime.executor.common.OffloadingExecutorEventType;
import org.apache.nemo.runtime.executor.common.controlmessages.offloading.SendToOffloadingWorker;
import org.apache.nemo.runtime.lambdaexecutor.ReadyTask;
import org.apache.nemo.runtime.lambdaexecutor.TaskMoveEvent;
import org.apache.nemo.runtime.lambdaexecutor.ThrottlingEvent;
import org.apache.nemo.runtime.lambdaexecutor.TaskEndEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStream;

public final class OffloadingExecutorInputDecoder implements OffloadingDecoder<Object> {
  private static final Logger LOG = LoggerFactory.getLogger(OffloadingExecutorInputDecoder.class.getName());


  public OffloadingExecutorInputDecoder() {
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
        return SendToOffloadingWorker.decode(inputStream);
      }
      case TASK_READY: {
        return ReadyTask.decode(inputStream);
      }
      case TASK_END: {
        final String taskId = dis.readUTF();
        return new TaskEndEvent(taskId);
      }
      case TASK_MOVE: {
        final String taskId = dis.readUTF();
        return new TaskMoveEvent(taskId);
      }
      case THROTTLE: {
        return new ThrottlingEvent();
      }
      default:
        throw new RuntimeException("Not supported type: " + et);
    }
  }

  @Override
  public Object decode(ByteBuf byteBuf) throws IOException {
    throw new UnsupportedOperationException();
  }
}
