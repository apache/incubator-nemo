package org.apache.nemo.common;

import javafx.util.Pair;
import org.apache.nemo.common.EventHandler;
import org.apache.nemo.common.OffloadingTransform;
import org.apache.nemo.common.OffloadingSerializer;
import org.apache.nemo.common.coder.DecoderFactory;
import org.apache.nemo.common.coder.EncoderFactory;

import java.util.List;
import java.util.concurrent.Future;

public interface ServerlessExecutorService<I> {

  // executor service 생성할때 code registration (worker initalization을 위한 용도)
  void execute(final I data);

  void shutdown();

  interface StatePartitioner<I, S> {

    List<S> getStatePartition();

    // state index
    List<Integer> routeToState(I input);

    EncoderFactory<S> getStateEncoderFactory();
    DecoderFactory<S> getStateDecoderFactory();
  }
}
