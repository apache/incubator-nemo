package org.apache.nemo.common;

import io.netty.buffer.ByteBuf;
import org.apache.nemo.common.coder.DecoderFactory;
import org.apache.nemo.common.coder.EncoderFactory;

import java.io.Closeable;
import java.io.Flushable;
import java.io.OutputStream;
import java.util.List;

public interface ServerlessExecutorService<I> {

  // executor service 생성할때 code registration (worker initalization을 위한 용도)
  void execute(final I data);
  void execute(final ByteBuf data);

  void shutdown();



  interface StatePartitioner<I, S> {

    List<S> getStatePartition();

    // state index
    List<Integer> routeToState(I input);

    EncoderFactory<S> getStateEncoderFactory();
    DecoderFactory<S> getStateDecoderFactory();
  }
}
