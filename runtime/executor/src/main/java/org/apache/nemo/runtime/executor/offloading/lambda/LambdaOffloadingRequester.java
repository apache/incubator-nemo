package org.apache.nemo.runtime.executor.offloading.lambda;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.services.lambda.AWSLambdaAsync;
import com.amazonaws.services.lambda.AWSLambdaAsyncClientBuilder;
import com.amazonaws.services.lambda.model.InvokeRequest;
import io.netty.channel.Channel;
import org.apache.nemo.common.NemoEvent;
import org.apache.nemo.runtime.executor.datatransfer.AWSUtils;
import org.apache.nemo.runtime.executor.offloading.NemoEventHandler;
import org.apache.nemo.runtime.executor.offloading.OffloadingRequester;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import static org.apache.nemo.common.lambda.Constants.LAMBDA_WARMUP;
import static org.apache.nemo.common.lambda.Constants.POOL_SIZE;

public final class LambdaOffloadingRequester implements OffloadingRequester {
  private static final Logger LOG = LoggerFactory.getLogger(LambdaOffloadingRequester.class.getName());

  private final ScheduledExecutorService warmer = Executors.newSingleThreadScheduledExecutor();


  private final AWSLambdaAsync awsLambda;
  private final NemoEventHandler nemoEventHandler;

  private final ExecutorService executorService = Executors.newCachedThreadPool();
  private final String serverAddress;
  private final int serverPort;

  public LambdaOffloadingRequester(final NemoEventHandler nemoEventHandler,
                                   final String serverAddress,
                                   final int port) {
    this.awsLambda = AWSLambdaAsyncClientBuilder.standard().withClientConfiguration(
      new ClientConfiguration().withMaxConnections(500)).build();
    this.nemoEventHandler =nemoEventHandler;
    this.serverAddress = serverAddress;
    this.serverPort = port;
  }

  @Override
  public void start() {
    this.warmer.scheduleAtFixedRate(() -> {
      //channelPool.clear();

      nemoEventHandler.getPendingRequest().addAndGet(LAMBDA_WARMUP);

      for (int i = 0; i < POOL_SIZE; i++) {
        executorService.submit(() -> {
          // Trigger lambdas
          final InvokeRequest request = new InvokeRequest()
            .withFunctionName(AWSUtils.SIDEINPUT_LAMBDA_NAME2)
            .withPayload(String.format("{\"address\":\"%s\", \"port\": %d}",
              serverAddress, serverPort));
          return awsLambda.invokeAsync(request);
        });
      }

      // take
      for (int i = 0; i < POOL_SIZE; i++) {
        try {
          //channelPool.add(nemoEventHandler.getHandshakeQueue().take().left());
          nemoEventHandler.getHandshakeQueue().take().left();
        } catch (InterruptedException e) {
          e.printStackTrace();
        }
      }

      LOG.info("Warmup end");

      while (nemoEventHandler.getPendingRequest().getAndDecrement() > 0) {
        try {
          final Channel channel = nemoEventHandler.getReadyQueue().take().left();
          channel.writeAndFlush(new NemoEvent(NemoEvent.Type.WARMUP_END, new byte[0], 0));
        } catch (InterruptedException e) {
          e.printStackTrace();
        }
      }

      LOG.info("Send Warmup end");
      nemoEventHandler.getPendingRequest().getAndIncrement();

    }, 0, LAMBDA_WARMUP, TimeUnit.SECONDS);
  }

  @Override
  public void createChannelRequest() {
    final InvokeRequest request = new InvokeRequest()
      .withFunctionName(AWSUtils.SIDEINPUT_LAMBDA_NAME2)
      .withPayload(String.format("{\"address\":\"%s\", \"port\": %d}",
        serverAddress, serverPort));
    awsLambda.invokeAsync(request);
  }

  @Override
  public void destroy() {

  }

  @Override
  public void close() {

  }
}
