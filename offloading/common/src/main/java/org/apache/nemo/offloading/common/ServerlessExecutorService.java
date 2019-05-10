package org.apache.nemo.offloading.common;

import io.netty.buffer.ByteBuf;

import java.util.List;

public interface ServerlessExecutorService<I, O> {

  // executor service 생성할때 code registration (worker initalization을 위한 용도)
  void execute(final I data);
  void execute(final ByteBuf data);
  void execute(final String id, final ByteBuf data, EventHandler<O> eventHandler);

  OffloadingWorker createStreamWorker();

  void shutdown();

  boolean isShutdown();

  boolean isFinished();
}
