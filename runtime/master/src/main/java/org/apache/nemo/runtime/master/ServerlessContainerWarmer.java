package org.apache.nemo.runtime.master;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.services.lambda.AWSLambdaAsync;
import com.amazonaws.services.lambda.AWSLambdaAsyncClientBuilder;
import com.amazonaws.services.lambda.model.InvokeRequest;
import io.netty.channel.Channel;
import io.netty.channel.group.ChannelGroup;
import io.netty.channel.group.DefaultChannelGroup;
import io.netty.util.concurrent.GlobalEventExecutor;
import org.apache.nemo.common.NemoEvent;
import org.apache.nemo.common.lambda.Constants;
import org.apache.nemo.runtime.common.offloading.NemoEventHandler;
import org.apache.nemo.runtime.common.offloading.NettyServerSideChannelHandler;
import org.apache.nemo.runtime.common.offloading.NettyServerTransport;
import org.apache.reef.wake.remote.ports.TcpPortProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.apache.nemo.common.lambda.Constants.POOL_SIZE;

public final class ServerlessContainerWarmer {
  private static final Logger LOG = LoggerFactory.getLogger(ServerlessContainerWarmer.class.getName());

  private final ChannelGroup serverChannelGroup = new DefaultChannelGroup(GlobalEventExecutor.INSTANCE);

  private final ExecutorService executorService = Executors.newCachedThreadPool();
  private final AtomicBoolean initialized = new AtomicBoolean(false);
  private final NemoEventHandler nemoEventHandler;

  private final NettyServerTransport nettyServerTransport;


  private final AWSLambdaAsync awsLambda;

  @Inject
  private ServerlessContainerWarmer(final TcpPortProvider tcpPortProvider) {
    this.nemoEventHandler = new NemoEventHandler();
    this.nettyServerTransport = new NettyServerTransport(
      tcpPortProvider, new NettyServerSideChannelHandler(serverChannelGroup, nemoEventHandler));
    this.awsLambda = AWSLambdaAsyncClientBuilder.standard().withClientConfiguration(
      new ClientConfiguration().withMaxConnections(500)).build();
  }

  public void start() {
    LOG.info("Warm up start: {}", POOL_SIZE);
    for (int i = 0; i < POOL_SIZE; i++) {
      executorService.submit(() -> {
        // Trigger lambdas
        final InvokeRequest request = new InvokeRequest()
          .withFunctionName(Constants.SIDEINPUT_LAMBDA_NAME2)
          .withPayload(String.format("{\"address\":\"%s\", \"port\": %d}",
            nettyServerTransport.getPublicAddress(), nettyServerTransport.getPort()));
        return awsLambda.invokeAsync(request);
      });
    }

    LOG.info("Waiting handshake queue");
    // take
    for (int i = 0; i < POOL_SIZE; i++) {
      try {
        //channelPool.add(nemoEventHandler.getHandshakeQueue().take().left());
        final Channel channel = nemoEventHandler.getHandshakeQueue().take().left();
        channel.writeAndFlush(new NemoEvent(NemoEvent.Type.WARMUP_END, new byte[0], 0));
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
    }
    LOG.info("End of waiting handshake queue");
  }
}
