package org.apache.nemo.runtime.executor.lambda;

import org.apache.nemo.common.coder.EncoderFactory;
import org.apache.nemo.runtime.executor.data.SerializerManager;

public interface StorageObjectFactory<T> {

  StorageObject<T> newInstance(String prefix,
                               String suffix,
                               int partition,
                               byte[] encodedDecoderFactory,
                               EncoderFactory encoderFactory);


  SideInputProcessor sideInputProcessor(SerializerManager serializerManager,
                                        String edgeId);

  public interface StorageObject<T> {

    void encode(T object);

    void close();

    int getPartition();

    String getPrefix();
  }
}
