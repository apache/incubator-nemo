package org.apache.nemo.offloading.client;

import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;

import java.util.List;
import java.util.concurrent.Future;

public interface OffloadingWorker<I, O> {

  Future<O> execute(ByteBuf input);
  Future<O> execute(I input);

  void finishOffloading();

  //<T> List<T> getResult();

  boolean isReady();

  boolean isFinished();

  Channel getChannel();
}
