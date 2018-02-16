package edu.snu.coral.runtime.executor.data.streamchainer;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

/**
 * A {@link StreamChainer} object indicates each stream manipulation strategy.
 * Stream can be chained by {@link StreamChainer} multiple times.
 */
public interface StreamChainer {
  /**
   * Chain {@link InputStream} and returns chained {@link InputStream}.
   *
   * @param in the stream which will be chained.
   * @return chained {@link InputStream}.
   * @throws IOException if fail to chain the stream.
   */
  InputStream chainInput(InputStream in) throws IOException;

  /**
   * Chain {@link OutputStream} and returns chained {@link OutputStream}.
   *
   * @param out the stream which will be chained.
   * @return chained {@link OutputStream}.
   * @throws IOException if fail to chain the stream.
   */
  OutputStream chainOutput(OutputStream out) throws IOException;
}
