package org.apache.nemo.offloading.client;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.services.lambda.AWSLambdaAsync;
import com.amazonaws.services.lambda.AWSLambdaAsyncClientBuilder;
import com.amazonaws.services.lambda.model.InvokeRequest;
import io.netty.channel.Channel;
import io.netty.channel.group.ChannelGroup;
import io.netty.channel.group.DefaultChannelGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.util.concurrent.DefaultThreadFactory;
import io.netty.util.concurrent.GlobalEventExecutor;
import org.apache.nemo.offloading.common.NettyChannelInitializer;
import org.apache.nemo.offloading.common.OffloadingEvent;
import org.apache.nemo.offloading.common.Constants;
import org.apache.reef.wake.remote.ports.TcpPortProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;

public final class ServerlessContainerWarmer {
  private static final Logger LOG = LoggerFactory.getLogger(ServerlessContainerWarmer.class.getName());

  private final ChannelGroup serverChannelGroup = new DefaultChannelGroup(GlobalEventExecutor.INSTANCE);

  private final ExecutorService executorService = Executors.newCachedThreadPool();
  private final AtomicBoolean initialized = new AtomicBoolean(false);
  private final OffloadingEventHandler nemoEventHandler;

  private final NettyServerTransport nettyServerTransport;


  private final AWSLambdaAsync awsLambda;

  @Inject
  private ServerlessContainerWarmer(final TcpPortProvider tcpPortProvider) {
    this.nemoEventHandler = new OffloadingEventHandler();
    this.nettyServerTransport = new NettyServerTransport(
      tcpPortProvider, new NettyChannelInitializer(
        new NettyServerSideChannelHandler(serverChannelGroup, nemoEventHandler)),
      new NioEventLoopGroup(3,
        new DefaultThreadFactory("Warmer")),
      false);
    this.awsLambda = AWSLambdaAsyncClientBuilder.standard().withRegion("ap-northeast-2")
      .withClientConfiguration(
      new ClientConfiguration().withMaxConnections(500)).build();
  }

  public void start(final int poolSize) {
    LOG.info("Warm up start: {}", poolSize);
    for (int i = 0; i < poolSize; i++) {
      final int index = i;
      executorService.submit(() -> {
        // Trigger lambdas
        final InvokeRequest request = new InvokeRequest()
          .withFunctionName(Constants.SIDEINPUT_LAMBDA_NAME2)
          .withPayload(String.format("{\"address\":\"%s\", \"port\": %d, \"requestId\": %d}",
            nettyServerTransport.getPublicAddress(), nettyServerTransport.getPort(), index));
        return awsLambda.invokeAsync(request);
      });
    }

    LOG.info("Waiting handshake queue");
    // take
    for (int i = 0; i < poolSize; i++) {
      try {
        //channelPool.add(nemoEventHandler.getHandshakeQueue().take().left());
        final Channel channel = nemoEventHandler.getHandshakeQueue().take().left();
        channel.writeAndFlush(new OffloadingEvent(OffloadingEvent.Type.WARMUP_END, new byte[0], 0));
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
    }
    LOG.info("End of waiting handshake queue");
  }
}
