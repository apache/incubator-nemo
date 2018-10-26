package org.apache.nemo.runtime.executor.data;

import java.io.IOException;
import java.io.InputStream;

/**
 * This class provide functionality to limit bytes read from {@link InputStream}.
 * You need to wrap chained compression stream with this stream to prevent overreading
 * inner stream.
 */
public final class LimitedInputStream extends InputStream {
  private final InputStream in;
  private long limit;

  /**
   * Constructor.
   *
   * @param in    {@link InputStream} that should be limited.
   * @param limit bytes to limit.
   */
  public LimitedInputStream(final InputStream in, final long limit) {
    this.in = in;
    this.limit = limit;
  }

  @Override
  public int read() throws IOException {
    if (limit > 0) {
      limit--;
      return in.read();
    } else {
      return -1;
    }
  }
}
