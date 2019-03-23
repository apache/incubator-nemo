package org.apache.nemo.offloading.common;

import io.netty.buffer.ByteBuf;

import java.util.List;

public interface ServerlessExecutorService<I> {

  // executor service 생성할때 code registration (worker initalization을 위한 용도)
  void execute(final I data);
  void execute(final ByteBuf data);

  OffloadingWorker createStreamWorker();

  void shutdown();

  boolean isShutdown();

  boolean isFinished();
}
