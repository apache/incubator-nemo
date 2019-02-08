package org.apache.nemo.runtime.executor.offloading;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.services.lambda.AWSLambdaAsync;
import com.amazonaws.services.lambda.AWSLambdaAsyncClientBuilder;
import com.amazonaws.services.lambda.model.InvokeRequest;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufOutputStream;
import io.netty.channel.Channel;
import io.netty.channel.group.ChannelGroup;
import io.netty.channel.group.DefaultChannelGroup;
import io.netty.util.concurrent.GlobalEventExecutor;
import org.apache.nemo.common.EventHandler;
import org.apache.nemo.common.NemoEvent;
import org.apache.nemo.common.OffloadingWorker;
import org.apache.nemo.common.OffloadingWorkerFactory;
import org.apache.nemo.common.coder.DecoderFactory;
import org.apache.nemo.common.coder.EncoderFactory;
import org.apache.nemo.runtime.common.offloading.NemoEventHandler;
import org.apache.nemo.runtime.common.offloading.NettyServerSideChannelHandler;
import org.apache.nemo.runtime.common.offloading.NettyServerTransport;
import org.apache.nemo.runtime.executor.datatransfer.AWSUtils;
import org.apache.nemo.runtime.executor.offloading.lambda.LambdaWorkerProxy;
import org.apache.reef.wake.remote.ports.TcpPortProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

public final class LambdaOffloadingWorkerFactory implements OffloadingWorkerFactory {
  private static final Logger LOG = LoggerFactory.getLogger(LambdaOffloadingWorkerFactory.class.getName());

  private final ChannelGroup serverChannelGroup = new DefaultChannelGroup(GlobalEventExecutor.INSTANCE);
  private NemoEventHandler nemoEventHandler;
  private final ConcurrentMap<Channel, EventHandler<NemoEvent>> channelEventHandlerMap;

  private final NettyServerTransport nettyServerTransport;
  private final ExecutorService executorService = Executors.newCachedThreadPool();

  private final AtomicBoolean initialized = new AtomicBoolean(false);
  private final AWSLambdaAsync awsLambda;

  private final AtomicInteger pendingRequest = new AtomicInteger(0);
  private final AtomicInteger extraRequest = new AtomicInteger(0);

  private final InvokeRequest request;

  @Inject
  private LambdaOffloadingWorkerFactory(final TcpPortProvider tcpPortProvider) {
    this.channelEventHandlerMap = new ConcurrentHashMap<>();
    this.nemoEventHandler = new NemoEventHandler(channelEventHandlerMap);
    this.nettyServerTransport = new NettyServerTransport(
      tcpPortProvider, new NettyServerSideChannelHandler(serverChannelGroup, nemoEventHandler));

    LOG.info("Netty server lambda transport created end");
    initialized.set(true);
    this.awsLambda = AWSLambdaAsyncClientBuilder.standard().withClientConfiguration(
      new ClientConfiguration().withMaxConnections(500)).build();

    this.request = new InvokeRequest()
      .withFunctionName(AWSUtils.SIDEINPUT_LAMBDA_NAME2)
      .withPayload(String.format("{\"address\":\"%s\", \"port\": %d}",
        nettyServerTransport.getPublicAddress(), nettyServerTransport.getPort()));
  }

  private void createChannelRequest() {
    pendingRequest.getAndIncrement();

    awsLambda.invokeAsync(request);
  }

  @Override
  public OffloadingWorker createOffloadingWorker(final List<String> serializedTransforms,
                                                 final EncoderFactory inputEncoderFactory,
                                                 final DecoderFactory inputDecoderFactory,
                                                 final EncoderFactory outputEncoderFactory,
                                                 final DecoderFactory outputDecoderFactory) {

    createChannelRequest();
    final Channel channel;
    try {
      channel = nemoEventHandler.getHandshakeQueue().take().left();
    } catch (InterruptedException e) {
      e.printStackTrace();
      throw new RuntimeException(e);
    }


    final ByteBuf buffer = channel.alloc().buffer();
    buffer.writeInt(NemoEvent.Type.WORKER_INIT.ordinal());

    ByteBufOutputStream bos = new ByteBufOutputStream(buffer);
    ObjectOutputStream oos = null;
    try {
      oos = new ObjectOutputStream(bos);
      oos.writeObject(serializedTransforms);
      oos.writeObject(inputDecoderFactory);
      oos.writeObject(outputEncoderFactory);
      oos.close();
      bos.close();
    } catch (final IOException e) {
      e.printStackTrace();
      throw new RuntimeException(e);
    }

    channel.writeAndFlush(new NemoEvent(NemoEvent.Type.WORKER_INIT, buffer));

    final int pendingNum = pendingRequest.decrementAndGet();
    if (pendingNum == 0) {
      executorService.execute(() -> {
        while (extraRequest.get() > 0) {
          LOG.info("Pending: {}, Extra: {}", pendingRequest.get(), extraRequest.get());
          if (extraRequest.getAndDecrement() > 0) {
            try {
              final Channel extraChannel = nemoEventHandler.getHandshakeQueue().take().left();
              extraChannel.writeAndFlush(new NemoEvent(NemoEvent.Type.WARMUP_END, new byte[0], 0));
            } catch (InterruptedException e) {
              e.printStackTrace();
              throw new RuntimeException(e);
            }
          } else {
            extraRequest.incrementAndGet();
          }
        }
      });
    }

    return new LambdaWorkerProxy(channel, this, channelEventHandlerMap, inputEncoderFactory,
      inputDecoderFactory, outputEncoderFactory, outputDecoderFactory);
  }

  @Override
  public void deleteOffloadingWorker(OffloadingWorker worker) {
    // extra request for pending job
    if (pendingRequest.get() > 0) {
      if (extraRequest.get() <= pendingRequest.get()) {
        extraRequest.getAndIncrement();
        LOG.info("Pending: {}, Increase Extra: {}", pendingRequest.get(), extraRequest.get());
        final InvokeRequest request = new InvokeRequest()
          .withFunctionName(AWSUtils.SIDEINPUT_LAMBDA_NAME2)
          .withPayload(String.format("{\"address\":\"%s\", \"port\": %d}",
            nettyServerTransport.getPublicAddress(), nettyServerTransport.getPort()));

        awsLambda.invokeAsync(request);
      }
    }
  }
}
