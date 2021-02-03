package org.apache.nemo.runtime.executor.offloading;

import io.netty.channel.Channel;
import org.apache.nemo.offloading.common.OffloadingEvent;
import org.apache.nemo.offloading.common.TaskHandlingEvent;
import org.apache.nemo.runtime.executor.common.Serializer;


public interface OffloadingWorker<I, O> {

  public double getLoad();

  String getId();

  void writeControl(OffloadingEvent offloadingEvent);
  void writeData(int pipeIndex, TaskHandlingEvent byteBuf);
  void writeSourceData(int pipeIndex,
                       Serializer serializer,
                       Object event);

  boolean isReady();

  boolean isFinished();

  Channel getChannel();

}
