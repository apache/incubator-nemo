package org.apache.nemo.runtime.executor.bytetransfer;

import java.io.IOException;

public interface OutputContext extends AutoCloseable {
  public TransferOutputStream newOutputStream() throws IOException;

  public void close() throws IOException;
}
