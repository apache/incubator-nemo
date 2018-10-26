package org.apache.nemo.runtime.executor.data.streamchainer;

import java.io.IOException;
import java.io.OutputStream;

/**
 * A {@link EncodeStreamChainer} object indicates each stream manipulation strategy.
 * Stream can be chained by {@link EncodeStreamChainer} multiple times.
 */
public interface EncodeStreamChainer {

  /**
   * Chain {@link OutputStream} and returns chained {@link OutputStream}.
   *
   * @param out the stream which will be chained.
   * @return chained {@link OutputStream}.
   * @throws IOException if fail to chain the stream.
   */
  OutputStream chainOutput(OutputStream out) throws IOException;
}
