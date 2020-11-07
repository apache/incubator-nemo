package org.apache.nemo.runtime.executor.bytetransfer;

import org.apache.nemo.runtime.executor.data.streamchainer.Serializer;

import java.io.IOException;

public interface TransferOutputStream extends AutoCloseable {
  public void writeElement(final Object element, final Serializer serializer);

  public void close() throws IOException;
}
