package org.apache.nemo.runtime.executor.data.streamchainer;

import java.io.IOException;
import java.io.InputStream;

/**
 * A {@link DecodeStreamChainer} object indicates each stream manipulation strategy.
 * Stream can be chained by {@link DecodeStreamChainer} multiple times.
 */
public interface DecodeStreamChainer {

  /**
   * Chain {@link InputStream} and returns chained {@link InputStream}.
   *
   * @param in the stream which will be chained.
   * @return chained {@link InputStream}.
   * @throws IOException if fail to chain the stream.
   */
  InputStream chainInput(InputStream in) throws IOException;
}
