package org.apache.nemo.runtime.executor.offloading.lambda.query8;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufOutputStream;
import io.netty.channel.Channel;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.sdk.values.KV;
import org.apache.commons.lang.SerializationUtils;
import org.apache.nemo.common.EventHandler;
import org.apache.nemo.common.GBKLambdaEvent;
import org.apache.nemo.common.NemoEvent;
import org.apache.nemo.common.Pair;
import org.apache.nemo.common.coder.DecoderFactory;
import org.apache.nemo.common.coder.EncoderFactory;
import org.apache.nemo.common.ir.OutputCollector;
import org.apache.nemo.common.ir.edge.executionproperty.DecoderProperty;
import org.apache.nemo.common.ir.edge.executionproperty.EncoderProperty;
import org.apache.nemo.common.ir.vertex.IRVertex;
import org.apache.nemo.common.ir.vertex.OperatorVertex;
import org.apache.nemo.common.punctuation.Watermark;
import org.apache.nemo.runtime.common.plan.RuntimeEdge;
import org.apache.nemo.runtime.executor.datatransfer.NextIntraTaskOperatorInfo;
import org.apache.nemo.runtime.executor.datatransfer.OutputWriter;
import org.apache.nemo.runtime.executor.offloading.LambdaChannelManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.*;

public class GBKLambdaEmitter<O> implements OutputCollector<O> {
  private static final Logger LOG = LoggerFactory.getLogger(GBKLambdaEmitter.class.getName());

  private final IRVertex irVertex;
  private final List<NextIntraTaskOperatorInfo> internalMainOutputs;
  private final Map<String, List<NextIntraTaskOperatorInfo>> internalAdditionalOutputs;
  private final List<OutputWriter> externalMainOutputs;
  private final Map<String, List<OutputWriter>> externalAdditionalOutputs;
  private final List<RuntimeEdge<IRVertex>> internalEdges;


  private final EncoderFactory encoderFactory;
  private final DecoderFactory decoderFactory;

  private static final int PARTITION_SIZE = 1000;

  private BlockingQueue<Channel> readyChannels;
  private List<Pair<Channel, ByteBuf>> channels;

  private int index = 0;
  private int numLambdas = 0;

  private final ExecutorService executorService = Executors.newCachedThreadPool();

  private final byte[] serializedDecoderFactory;
  private long startTime;
  private final LambdaChannelManager lambdaTransport;
  private GBKChannelHandler channelHandler;

  private boolean toLambda = false;

  //private final LambdaOffloadingRequester warmer;

  public GBKLambdaEmitter(final IRVertex irVertex,
                          final List<NextIntraTaskOperatorInfo> internalMainOutputs,
                          final Map<String, List<NextIntraTaskOperatorInfo>> internalAdditionalOutputs,
                          final List<OutputWriter> externalMainOutputs,
                          final Map<String, List<OutputWriter>> externalAdditionalOutputs,
                          final List<RuntimeEdge<IRVertex>> internalEdges,
                          final LambdaChannelManager lambdaChannelManager) {
    this.irVertex = irVertex;
    this.internalMainOutputs = internalMainOutputs;
    this.internalAdditionalOutputs = internalAdditionalOutputs;
    this.externalMainOutputs = externalMainOutputs;
    this.externalAdditionalOutputs = externalAdditionalOutputs;
    this.internalEdges = internalEdges;
    this.encoderFactory = internalEdges.get(0).getPropertyValue(EncoderProperty.class).get();
    this.decoderFactory = internalEdges.get(0).getPropertyValue(DecoderProperty.class).get();
    this.serializedDecoderFactory = SerializationUtils.serialize(decoderFactory);
    this.lambdaTransport = lambdaChannelManager;
    //this.warmer = new LambdaOffloadingRequester();
    //warmer.warmup();
  }

  private void emit(final OperatorVertex vertex, final O output) {
    vertex.getTransform().onData(output);
  }

  @Override
  public void emit(O output) {
    final WindowedValue<KV<Object, Iterable>> value = (WindowedValue<KV<Object, Iterable>>) output;

    if (value.getValue().getKey() instanceof GBKLambdaEvent) {
      final GBKLambdaEvent event = (GBKLambdaEvent) value.getValue().getKey();
      if (event.type.equals(GBKLambdaEvent.Type.START)) {

        final Integer numKeys = (Integer) event.data;
        if (numKeys < PARTITION_SIZE) {
          LOG.info("toLambda: false");
          toLambda = false;
          return;
        } else {
          LOG.info("toLambda: true");
          toLambda = true;
        }

        startTime = System.currentTimeMillis();

        index = 0;
        numLambdas = 0;

        final CountDownLatch latch = new CountDownLatch(numLambdas);
        channelHandler = new GBKChannelHandler(latch);

        // TODO: start lambda
        // number of lambdas
        numLambdas = numKeys / PARTITION_SIZE + (numKeys % PARTITION_SIZE == 0 ? 0 : 1);
        LOG.info("GBK Star, invoke {}, keys: {}", numLambdas, numKeys);
        readyChannels = new LinkedBlockingQueue<>();
        channels = new ArrayList<>(numLambdas);
        for (int i = 0; i < numLambdas; i++) {
          executorService.submit(() -> {
            try {
              // TODO: fix!!!
              final Channel channel = lambdaTransport.createOffloadingChannel(new LinkedList<>()).get();
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

        if (!toLambda) {
          return;
        }

        LOG.info("Receive END of GBK");

        while (channels.size() < numLambdas) {
          // wait
          try {
            LOG.info("Waiting end ... {}/{}", channels.size(), numLambdas);
            final Channel channel = readyChannels.take();
            channels.add(Pair.of(channel, channel.alloc().ioBuffer()));
          } catch (InterruptedException e) {
            e.printStackTrace();
          }
        }

        for (final Pair<Channel, ByteBuf> pair : channels) {
          final Channel channel = pair.left();
          final ByteBuf buffer = pair.right();
          channel.write(new NemoEvent(NemoEvent.Type.GBK, buffer));
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

      if (!toLambda) {
        for (final NextIntraTaskOperatorInfo internalVertex : internalMainOutputs) {
          emit(internalVertex.getNextOperator(), output);
        }

        return;
      }

      // TODO: serialize data and send it to lambda
      Channel channel = null;
      if (channels.size() < numLambdas) {
        try {
          channel = readyChannels.take();
          channels.add(Pair.of(channel, channel.alloc().ioBuffer()));
        } catch (InterruptedException e) {
          e.printStackTrace();
        }
      }

      channel = channels.get(index).left();
      index = (index + 1) % channels.size();

      final ByteBuf buffer = channels.get(index).right();
      buffer.writeInt(NemoEvent.Type.GBK.ordinal());
      final ByteBufOutputStream byteBufOutputStream =
        new ByteBufOutputStream(buffer);

      try {
        final EncoderFactory.Encoder encoder = encoderFactory.create(byteBufOutputStream);
        encoder.encode(output);
        byteBufOutputStream.close();

        // write data!
        //channel.write(new NemoEvent(NemoEvent.Type.GBK, buffer));

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
