package org.apache.nemo.runtime.executor.datatransfer;

import org.apache.nemo.common.coder.EncoderFactory;

public interface StorageObjectFactory<T> {

  StorageObject<T> newInstance(String prefix,
                               int partition,
                               byte[] encodedDecoderFactory,
                               EncoderFactory encoderFactory);

  public interface StorageObject<T> {

    void encode(T object);

    void close();

    int getPartition();

    String getPrefix();
  }
}
