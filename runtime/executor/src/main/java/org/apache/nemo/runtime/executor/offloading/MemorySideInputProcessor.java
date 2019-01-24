package org.apache.nemo.runtime.executor.offloading;

import io.netty.channel.Channel;
import org.apache.commons.lang.SerializationUtils;
import org.apache.nemo.common.DirectByteArrayOutputStream;
import org.apache.nemo.common.NemoEvent;
import org.apache.nemo.common.coder.DecoderFactory;
import org.apache.nemo.common.coder.EncoderFactory;
import org.apache.nemo.runtime.executor.data.SerializerManager;
import org.apache.nemo.runtime.executor.datatransfer.NemoEventDecoderFactory;
import org.apache.nemo.runtime.executor.datatransfer.NemoEventEncoderFactory;
import org.apache.nemo.common.lambda.LambdaDecoderFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

public final class MemorySideInputProcessor implements SideInputProcessor {
  private static final Logger LOG = LoggerFactory.getLogger(MemorySideInputProcessor.class.getName());

  private final ExecutorService executorService = Executors.newCachedThreadPool();
  private final EncoderFactory encoderFactory;
  private EncoderFactory.Encoder encoder = null;
  private final DecoderFactory decoderFactory;
  private final byte[] sideInputDecodedFactory;
  private final ConcurrentMap<String, ConcurrentLinkedQueue<MemoryStorageObject>> prefixAndObjectMap;
  private final ConcurrentMap<String, AtomicInteger> prefixAndSizeMap;
  private final NettyServerTransport lambdaTransport;
  private final List<String> serializedVertices;

  public MemorySideInputProcessor(final SerializerManager serializerManager,
                                  final String edgeId,
                                  final ConcurrentMap<String, ConcurrentLinkedQueue<MemoryStorageObject>> prefixAndObjectMap,
                                  final ConcurrentMap<String, AtomicInteger> prefixAndSizeMap,
                                  final NettyServerTransport lambdaTransport,
                                  final List<String> serializedVertices) {
    this.encoderFactory = ((NemoEventEncoderFactory) serializerManager.getSerializer(edgeId)
      .getEncoderFactory()).getValueEncoderFactory();
    this.decoderFactory = new LambdaDecoderFactory(
      ((NemoEventDecoderFactory) serializerManager.getSerializer(edgeId)
        .getDecoderFactory()).getValueDecoderFactory());
    this.sideInputDecodedFactory = SerializationUtils.serialize(decoderFactory);
    this.prefixAndObjectMap = prefixAndObjectMap;
    this.prefixAndSizeMap = prefixAndSizeMap;
    this.lambdaTransport = lambdaTransport;
    this.serializedVertices = serializedVertices;
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
        futures.add(lambdaTransport.createOffloadingChannel(serializedVertices));
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

      LOG.info("Latency {} of {}", System.currentTimeMillis() - startTime, sideInputKey);
      System.out.println("Latency " + (System.currentTimeMillis() - startTime) + " of " + sideInputKey);
      prefixAndObjectMap.remove(sideInputKey);
      prefixAndSizeMap.remove(sideInputKey);

      return results;
    }
  }
