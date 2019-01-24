package org.apache.nemo.runtime.executor.lambda.query7;

import org.apache.nemo.common.coder.EncoderFactory;
import org.apache.nemo.runtime.executor.data.SerializerManager;
import org.apache.nemo.runtime.executor.lambda.NettyServerLambdaTransport;
import org.apache.nemo.runtime.executor.lambda.SideInputProcessor;
import org.apache.nemo.runtime.executor.lambda.StorageObjectFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

public final class MemoryStorageObjectFactory implements StorageObjectFactory {
  private static final Logger LOG = LoggerFactory.getLogger(MemoryStorageObjectFactory.class.getName());

  public static final MemoryStorageObjectFactory INSTACE = new MemoryStorageObjectFactory();

  private ConcurrentMap<String, ConcurrentLinkedQueue<MemoryStorageObject>> prefixAndObjectMap;
  private ConcurrentMap<String, AtomicInteger> prefixAndSizeMap;

  private NettyServerLambdaTransport lambdaTransport;
  private boolean initialized = false;

  private List<String> serializedVertices;

  private MemoryStorageObjectFactory() {

  }

  private synchronized void lazyInit() {
    if (!initialized) {
      this.prefixAndObjectMap = new ConcurrentHashMap<>();
      this.prefixAndSizeMap = new ConcurrentHashMap<>();
      this.lambdaTransport = NettyServerLambdaTransport.INSTANCE;
      initialized = true;
    }
  }

  @Override
  public synchronized void setSerializedVertices(List sv) {
    serializedVertices = sv;
  }

  @Override
  public StorageObject newInstance(String prefix,
                                   String suffix,
                                   int partition,
                                   byte[] encodedDecoderFactory,
                                   EncoderFactory encoderFactory) {

    lazyInit();
    final MemoryStorageObject memoryStorageObject =
      new MemoryStorageObject(prefix+suffix, partition, encodedDecoderFactory, encoderFactory);
    prefixAndObjectMap.putIfAbsent(prefix, new ConcurrentLinkedQueue<>());
    prefixAndObjectMap.get(prefix).add(memoryStorageObject);
    prefixAndSizeMap.putIfAbsent(prefix, new AtomicInteger(0));
    prefixAndSizeMap.get(prefix).getAndIncrement();
    return memoryStorageObject;
  }

  public SideInputProcessor sideInputProcessor(SerializerManager serializerManager,
                                               String edgeId) {
    lazyInit();
    return new LambdaSideInputProcessor(serializerManager, edgeId,
      prefixAndObjectMap, prefixAndSizeMap, lambdaTransport, serializedVertices);
    //return new VMSideInputProcessor(serializerManager, edgeId,
    //  prefixAndObjectMap, prefixAndSizeMap, lambdaTransport, serializedVertices);
  }
}



