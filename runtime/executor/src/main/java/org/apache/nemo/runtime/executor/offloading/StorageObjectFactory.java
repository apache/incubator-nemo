package org.apache.nemo.runtime.executor.offloading;

import org.apache.nemo.common.coder.EncoderFactory;
import org.apache.nemo.runtime.executor.data.SerializerManager;
import org.apache.reef.tang.annotations.DefaultImplementation;

import java.util.List;

@DefaultImplementation(MemoryStorageObjectFactory.class)
public interface StorageObjectFactory<T> {

  void setSerializedVertices(List<String> serializedVertices);

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
