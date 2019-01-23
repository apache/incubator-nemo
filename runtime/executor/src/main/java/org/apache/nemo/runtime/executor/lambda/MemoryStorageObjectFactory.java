package org.apache.nemo.runtime.executor.lambda;

import io.netty.channel.Channel;
import org.apache.commons.lang.SerializationUtils;
import org.apache.nemo.common.*;
import org.apache.nemo.common.coder.DecoderFactory;
import org.apache.nemo.common.coder.EncoderFactory;
import org.apache.nemo.runtime.executor.data.SerializerManager;
import org.apache.nemo.runtime.executor.datatransfer.NemoEventDecoderFactory;
import org.apache.nemo.runtime.executor.datatransfer.NemoEventEncoderFactory;
import org.apache.nemo.runtime.lambda.LambdaDecoderFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
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
  public void setSerializedVertices(List sv) {
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
    return new MemorySideInputProcessor(serializerManager, edgeId);
  }

  final class MemoryStorageObject implements StorageObject {
    private final EncoderFactory.Encoder encoder;
    private final DirectByteArrayOutputStream outputStream;
    public final String fname;
    private final AtomicBoolean closed = new AtomicBoolean(false);
    private boolean finished = false;
    private final int partition;
    private final String prefix;
    int cnt = 0;
    public MemoryStorageObject(final String prefix, final int partition,
                               final byte[] encodedDecoderFactory,
                               final EncoderFactory encoderFactory) {
      this.prefix = prefix;
      this.fname = prefix + "-" + partition;
      this.partition = partition;
      this.outputStream = new DirectByteArrayOutputStream();
      try {
        outputStream.write(encodedDecoderFactory);
        this.encoder = encoderFactory.create(outputStream);
      } catch (IOException e) {
        e.printStackTrace();
        throw new RuntimeException(e);
      }
    }

    @Override
    public void encode(Object object) {
      try {
        cnt += 1;
        encoder.encode(object);
      } catch (IOException e) {
        e.printStackTrace();
        throw new RuntimeException(e);
      }
    }

    @Override
    public void close() {
      if (closed.compareAndSet(false, true)) {
        try {
          outputStream.close();
          finished = true;
        } catch (IOException e) {
          e.printStackTrace();
          throw new RuntimeException(e);
        }
      }
    }

    @Override
    public int getPartition() {
      return partition;
    }

    @Override
    public String getPrefix() {
      return prefix;
    }

    @Override
    public String toString() {
      return fname;
    }
  }


  final class MemorySideInputProcessor implements SideInputProcessor {
    private final ExecutorService executorService = Executors.newCachedThreadPool();
    private final EncoderFactory encoderFactory;
    private EncoderFactory.Encoder encoder = null;
    private final DecoderFactory decoderFactory;
    private final byte[] sideInputDecodedFactory;

    MemorySideInputProcessor(final SerializerManager serializerManager,
                             final String edgeId) {
      this.encoderFactory = ((NemoEventEncoderFactory) serializerManager.getSerializer(edgeId)
        .getEncoderFactory()).getValueEncoderFactory();
      this.decoderFactory = new LambdaDecoderFactory(
        ((NemoEventDecoderFactory) serializerManager.getSerializer(edgeId)
          .getDecoderFactory()).getValueDecoderFactory());
      this.sideInputDecodedFactory = SerializationUtils.serialize(decoderFactory);
    }

    @Override
    public Object processSideAndMainInput(Object output, String sideInputKey) {
      // Invoke lambdas

      final long startTime = System.currentTimeMillis();

      final ConcurrentLinkedQueue<MemoryStorageObject> queue = prefixAndObjectMap.get(sideInputKey);
      final int size = prefixAndSizeMap.get(sideInputKey).get();

      LOG.info("Side Input Key: {}", sideInputKey);
      final List<MemoryStorageObject> list = new ArrayList<>(size+5);
      while (!queue.isEmpty()) {
        final MemoryStorageObject object = queue.poll();
        list.add(object);
      }

      LOG.info("MemoryStorageObject size: {}", list.size());

      final List<Future<Channel>> futures = new ArrayList<>(list.size());
      // 0. Trigger lambdas
      for (int i = 0; i < list.size(); i++) {
        futures.add(lambdaTransport.createLambdaChannel(serializedVertices));
      }

      final DirectByteArrayOutputStream bos = new DirectByteArrayOutputStream();
      try {
        bos.write(sideInputDecodedFactory);
        final EncoderFactory.Encoder encoder = encoderFactory.create(bos);
        encoder.encode(output);
        bos.close();
      } catch (final IOException e) {
        throw new RuntimeException(e);
      }

      final byte[] sideInputBytes = bos.getBufDirectly();

      LOG.info("End of invocation of lambdas: {}, {}", list.size(),
        System.currentTimeMillis() - startTime);
      final List<Channel> channels = new ArrayList<>(list.size());

      // 1. lambda initialized
      for (int i = 0; i < list.size(); i++) {
        try {
          final Channel channel = futures.get(i).get();
          final int ind = i;
          executorService.submit(() -> {
            // 2. send side input
            //LOG.info("Write side input: {}", sideInputBytes);
            LOG.info("Write side input to {}", channel);
            channel.writeAndFlush(new NemoEvent(NemoEvent.Type.SIDE, sideInputBytes, bos.getCount()));

            // 3. send main inputs
            final MemoryStorageObject obj = list.get(ind);
            obj.close();
            channel.writeAndFlush(new NemoEvent(NemoEvent.Type.MAIN,
              obj.outputStream.getBufDirectly(), obj.outputStream.getCount()));
            LOG.info("Write {} main input to {}", obj.cnt, channel);
          });
        } catch (InterruptedException e) {
          e.printStackTrace();
          throw new RuntimeException(e);
        } catch (ExecutionException e) {
          e.printStackTrace();
          throw new RuntimeException(e);
        }
      }

      LOG.info("{}: # of invocations: {}, # of message send: {}",
        System.currentTimeMillis() - startTime, list.size(), channels.size());

      final List<Object> results = new ArrayList<>(size);
      for (int i = 0; i < size; i++) {
        results.add(lambdaTransport.takeResult());
      }


      /*
      // wait results
      final List<Object> results = futures.stream().map(future -> {
        try {
          return future.get().toString();
        } catch (InterruptedException e) {
          e.printStackTrace();
          throw new RuntimeException(e);
        } catch (ExecutionException e) {
          e.printStackTrace();
          throw new RuntimeException(e);
        }
      }).collect(Collectors.toList());
      */

      LOG.info("Receive all results: {}", System.currentTimeMillis() - startTime);
      prefixAndObjectMap.remove(sideInputKey);
      prefixAndSizeMap.remove(sideInputKey);

      return results;
    }
  }

}



