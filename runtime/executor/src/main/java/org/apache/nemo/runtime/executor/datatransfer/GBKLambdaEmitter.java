package org.apache.nemo.runtime.executor.datatransfer;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufOutputStream;
import io.netty.channel.Channel;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.sdk.values.KV;
import org.apache.commons.lang.SerializationUtils;
import org.apache.nemo.common.EventHandler;
import org.apache.nemo.common.GBKLambdaEvent;
import org.apache.nemo.common.NemoEvent;
import org.apache.nemo.common.coder.DecoderFactory;
import org.apache.nemo.common.coder.EncoderFactory;
import org.apache.nemo.common.ir.OutputCollector;
import org.apache.nemo.common.punctuation.Watermark;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

public class GBKLambdaEmitter implements OutputCollector {
  private static final Logger LOG = LoggerFactory.getLogger(GBKLambdaEmitter.class.getName());

  private final EncoderFactory encoderFactory;
  private final DecoderFactory decoderFactory;

  private static final int PARTITION_SIZE = 9000;

  private BlockingQueue<Channel> readyChannels;
  private List<Channel> channels;

  private boolean initialized = false;
  private int index = 0;
  private int numLambdas = 0;

  private final ExecutorService executorService = Executors.newCachedThreadPool();

  private final byte[] serializedDecoderFactory;
  private long startTime;
  private final NettyServerLambdaTransport lambdaTransport;
  private GBKChannelHandler channelHandler;
  private final AtomicInteger numResults = new AtomicInteger(0);

  private final LambdaWarmer warmer;

  public GBKLambdaEmitter(final EncoderFactory encoderFactory,
                          final DecoderFactory decoderFactory) {
    this.encoderFactory = encoderFactory;
    this.decoderFactory = decoderFactory;
    this.serializedDecoderFactory = SerializationUtils.serialize(decoderFactory);
    this.lambdaTransport = NettyServerLambdaTransport.INSTANCE;
    this.warmer = new LambdaWarmer();
    warmer.warmup();
  }

  @Override
  public void emit(Object output) {
    final WindowedValue<KV<Object, Iterable>> value = (WindowedValue<KV<Object, Iterable>>) output;

    if (value.getValue().getKey() instanceof GBKLambdaEvent) {
      final GBKLambdaEvent event = (GBKLambdaEvent) value.getValue().getKey();
      if (event.type.equals(GBKLambdaEvent.Type.START)) {

        startTime = System.currentTimeMillis();

        index = 0;
        numLambdas = 0;

        final CountDownLatch latch = new CountDownLatch(numLambdas);
        channelHandler = new GBKChannelHandler(latch);

        // TODO: start lambda
        // number of lambdas
        final Integer numKeys = (Integer) event.data;
        numLambdas = numKeys / PARTITION_SIZE + (numKeys % PARTITION_SIZE == 0 ? 0 : 1);
        LOG.info("GBK Star, invoke {}, keys: {}", numLambdas, numKeys);
        readyChannels = new LinkedBlockingQueue<>();
        channels = new ArrayList<>(numLambdas);
        for (int i = 0; i < numLambdas; i++) {
          executorService.submit(() -> {
            try {
              final Channel channel = lambdaTransport.createLambdaChannel().get();
              lambdaTransport.setChannelHandler(channel, channelHandler);
              channel.writeAndFlush(new NemoEvent(NemoEvent.Type.GBK_START,
                serializedDecoderFactory, serializedDecoderFactory.length));
              readyChannels.add(channel);
            } catch (InterruptedException e) {
              e.printStackTrace();
              throw new RuntimeException(e);
            } catch (ExecutionException e) {
              e.printStackTrace();
              throw new RuntimeException(e);
            }
          });
        }

      } else if (event.type.equals(GBKLambdaEvent.Type.END)) {
        // TODO: end lambda
        // flush all data
        // wait for results

        LOG.info("Receive END of GBK");

        while (channels.size() < numLambdas) {
          // wait
          try {
            LOG.info("Waiting end ... {}/{}", channels.size(), numLambdas);
            final Channel channel = readyChannels.take();
            channels.add(channel);
          } catch (InterruptedException e) {
            e.printStackTrace();
          }
        }

        for (final Channel channel : channels) {
          channel.writeAndFlush(new NemoEvent(NemoEvent.Type.END, new byte[0], 0));
        }

        // waiting end ...
        LOG.info("Waiting result...");
        try {
          channelHandler.latch.await();
        } catch (InterruptedException e) {
          e.printStackTrace();
        }

        LOG.info("End of receive result");
        LOG.info("GBK latency: {}:w", (System.currentTimeMillis() - startTime));
        System.out.println("GBK latency: " + (System.currentTimeMillis() - startTime));
      }
    } else {
      // TODO: serialize data and send it to lambda
      Channel channel = null;
      if (channels.size() < numLambdas) {
        try {
          channel = readyChannels.take();
          channels.add(channel);
        } catch (InterruptedException e) {
          e.printStackTrace();
        }
      }

      channel = channels.get(index);
      index = (index + 1) % channels.size();

      final ByteBuf buffer = channel.alloc().buffer();
      buffer.writeInt(NemoEvent.Type.GBK.ordinal());
      final ByteBufOutputStream byteBufOutputStream =
        new ByteBufOutputStream(buffer);

      try {
        final EncoderFactory.Encoder encoder = encoderFactory.create(byteBufOutputStream);
        encoder.encode(output);
        byteBufOutputStream.close();

        // write data!
        channel.write(new NemoEvent(NemoEvent.Type.GBK, buffer));

      } catch (IOException e) {
        e.printStackTrace();
        throw new RuntimeException(e);
      }
    }
  }

  @Override
  public void emitWatermark(Watermark watermark) {

  }

  @Override
  public void emit(String dstVertexId, Object output) {

  }

  final class GBKChannelHandler implements EventHandler<NemoEvent> {
    private final CountDownLatch latch;

    public GBKChannelHandler(final CountDownLatch latch) {
      this.latch = latch;
    }

    @Override
    public void onNext(NemoEvent msg) {
      if (msg.getType().equals(NemoEvent.Type.RESULT)) {
        LOG.info("Receive result: {}", msg.getByteBuf());
        latch.countDown();
      }
    }
  }
}
