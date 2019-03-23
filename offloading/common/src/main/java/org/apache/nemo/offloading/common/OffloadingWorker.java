package org.apache.nemo.offloading.common;

import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;
import org.apache.nemo.offloading.common.Pair;

import java.util.Optional;
import java.util.concurrent.Future;

public interface OffloadingWorker<I, O> {

  String getId();

  Future<Optional<O>> execute(ByteBuf input, int dataId, boolean speculative);
  //Future<O> execute(I input);

  <T> T finishOffloading();

  //<T> List<T> getResult();

  boolean isReady();

  boolean isFinished();

  Channel getChannel();

  Pair<ByteBuf, Integer> getCurrentProcessingInput();

  int getDataProcessingCnt();
}
