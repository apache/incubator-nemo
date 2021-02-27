package org.apache.nemo.offloading.common;

import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;

import java.util.Optional;
import java.util.concurrent.Future;

public interface DeprecatedOffloadingWorker<I, O> {

  public double getLoad();

  String getId();

  void writeControl(OffloadingMasterEvent offloadingEvent);
  void writeData(int pipeIndex, TaskHandlingEvent byteBuf);

  @Deprecated
  Future<Optional<O>> execute(ByteBuf input, int dataId, boolean speculative);
  //Future<O> execute(I input);

  <T> T finishOffloading();

  void forceClose();

  //<T> List<T> getResult();

  boolean isReady();

  boolean isFinished();

  Channel getChannel();

  Pair<ByteBuf, Integer> getCurrentProcessingInput();

  int getDataProcessingCnt();
}
