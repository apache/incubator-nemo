package org.apache.nemo.offloading.client;

import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;
import org.apache.nemo.common.Pair;

import java.util.List;
import java.util.Optional;
import java.util.concurrent.Future;

public interface OffloadingWorker<I, O> {

  Pair<Future<Optional<O>>, Boolean> execute(ByteBuf input, int dataId, boolean speculative);
  //Future<O> execute(I input);

  void finishOffloading();

  //<T> List<T> getResult();

  boolean isReady();

  boolean isFinished();

  Channel getChannel();

  Pair<ByteBuf, Integer> getCurrentProcessingInput();

  int getDataProcessingCnt();
}
