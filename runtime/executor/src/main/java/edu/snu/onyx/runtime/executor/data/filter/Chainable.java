package edu.snu.onyx.runtime.executor.data.filter;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

/**
 * A {@link Chainable} object indicates each stream manipulation strategy.
 * Stream can be chained by {@link Chainable} multiple times.
 */
public interface Chainable {
  /**
   * Wrap {@link InputStream} and returns wrapped {@link InputStream}.
   *
   * @param in the stream which will be wrapped.
   * @return wrapped {@link InputStream}.
   * @throws IOException if fail to wrap the stream.
   */
  InputStream wrapInput(InputStream in) throws IOException;

  /**
   * Wrap {@link OutputStream} and returns wrapped {@link OutputStream}.
   *
   * @param out the stream which will be wrapped.
   * @return wrapped {@link OutputStream}.
   * @throws IOException if fail to wrap the stream.
   */
  OutputStream wrapOutput(OutputStream out) throws IOException;
}
