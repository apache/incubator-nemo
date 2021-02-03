package org.apache.nemo.offloading.common;

import io.netty.buffer.ByteBuf;

public interface ServerlessExecutorService<I, O> {

  // executor service 생성할때 code registration (worker initalization을 위한 용도)
  void execute(final ByteBuf data);
  void execute(final String id, final ByteBuf data, EventHandler<O> eventHandler);

  DeprecatedOffloadingWorker createStreamWorker();

  void shutdown();

  boolean isShutdown();

  boolean isFinished();
}
