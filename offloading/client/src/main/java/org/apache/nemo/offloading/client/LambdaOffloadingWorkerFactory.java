package org.apache.nemo.offloading.client;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.services.lambda.AWSLambdaAsync;
import com.amazonaws.services.lambda.AWSLambdaAsyncClientBuilder;
import com.amazonaws.services.lambda.model.InvokeRequest;
import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;
import io.netty.channel.group.ChannelGroup;
import io.netty.channel.group.DefaultChannelGroup;
import io.netty.util.concurrent.GlobalEventExecutor;
import org.apache.nemo.common.*;
import org.apache.reef.wake.remote.ports.TcpPortProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
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
  public OffloadingWorker createOffloadingWorker(final ByteBuf workerInitBuffer,
                                                 final OffloadingSerializer offloadingSerializer) {

    createChannelRequest();

    final Future<Channel> channelFuture = new Future<Channel>() {
      @Override
      public boolean cancel(boolean mayInterruptIfRunning) {
        return false;
      }

      @Override
      public boolean isCancelled() {
        return false;
      }

      @Override
      public boolean isDone() {
        return false;
      }

      @Override
      public Channel get() throws InterruptedException, ExecutionException {
        final Channel channel;
        try {
          channel = nemoEventHandler.getHandshakeQueue().take().left();
        } catch (InterruptedException e) {
          e.printStackTrace();
          throw new RuntimeException(e);
        }

        channel.writeAndFlush(new NemoEvent(NemoEvent.Type.WORKER_INIT, workerInitBuffer));

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

        return channel;
      }

      @Override
      public Channel get(long timeout, TimeUnit unit) throws InterruptedException, ExecutionException, TimeoutException {
        return null;
      }
    };

    return new LambdaWorkerProxy(channelFuture, this, channelEventHandlerMap,
      offloadingSerializer.getInputEncoder(), offloadingSerializer.getOutputDecoder());
  }

  @Override
  public void deleteOffloadingWorker(OffloadingWorker worker) {
    // work stealing here!


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
