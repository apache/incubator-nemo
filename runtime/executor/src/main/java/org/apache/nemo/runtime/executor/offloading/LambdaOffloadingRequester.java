package org.apache.nemo.runtime.executor.offloading;


import com.amazonaws.ClientConfiguration;
import com.amazonaws.services.lambda.AWSLambdaAsync;
import com.amazonaws.services.lambda.AWSLambdaAsyncClientBuilder;
import com.amazonaws.services.lambda.model.InvokeRequest;
import com.amazonaws.services.lambda.model.InvokeResult;
import io.netty.channel.Channel;
import org.apache.nemo.offloading.client.AWSUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;

public final class LambdaOffloadingRequester implements OffloadingRequester {

  private static final Logger LOG = LoggerFactory.getLogger(LambdaOffloadingRequester.class.getName());

  private final AtomicInteger requestId = new AtomicInteger();
  private final String address;
  private final int port;
  private final AWSLambdaAsync awsLambda;

  public LambdaOffloadingRequester(final String address,
                                   final int port,
                                   final String region) {
    this.address = address;
    this.port = port;
    this.awsLambda = AWSLambdaAsyncClientBuilder.standard()
      .withRegion(region).withClientConfiguration(
        new ClientConfiguration().withMaxConnections(500)).build();
  }


  @Override
  public void start() {

  }

  @Override
  public void createChannelRequest() {
    final InvokeRequest request = new InvokeRequest()
      .withFunctionName(AWSUtils.SIDEINPUT_LAMBDA_NAME2)
      .withPayload(String.format("{\"address\":\"%s\", \"port\": %d, \"requestId\": %d}",
        address, port, requestId.getAndIncrement()));

    LOG.info("Invoke async request {}", request);
    final Future<InvokeResult> future = awsLambda.invokeAsync(request);
    try {
      final InvokeResult result = future.get();
      LOG.info("Invoke result: {}", result);
    } catch (Exception e) {
      e.printStackTrace();
      throw new RuntimeException(e);
    }
  }

  @Override
  public void destroy() {

  }

  @Override
  public void close() {

  }

  @Override
  public void destroyChannel(Channel channel) {

  }
}
