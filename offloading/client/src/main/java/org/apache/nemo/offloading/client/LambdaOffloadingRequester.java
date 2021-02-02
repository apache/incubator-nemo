package org.apache.nemo.offloading.client;


import com.amazonaws.ClientConfiguration;
import com.amazonaws.services.ec2.AmazonEC2;
import com.amazonaws.services.ec2.AmazonEC2ClientBuilder;
import com.amazonaws.services.ec2.model.*;
import com.amazonaws.services.lambda.AWSLambdaAsync;
import com.amazonaws.services.lambda.AWSLambdaAsyncClientBuilder;
import com.amazonaws.services.lambda.model.InvokeRequest;
import io.netty.bootstrap.Bootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.group.ChannelGroup;
import io.netty.channel.group.DefaultChannelGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.util.concurrent.DefaultThreadFactory;
import io.netty.util.concurrent.GlobalEventExecutor;
import org.apache.nemo.offloading.common.EventHandler;
import org.apache.nemo.offloading.common.NettyChannelInitializer;
import org.apache.nemo.offloading.common.NettyLambdaInboundHandler;
import org.apache.nemo.offloading.common.OffloadingEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.apache.nemo.offloading.common.Constants.VM_WORKER_PORT;

public final class LambdaOffloadingRequester implements OffloadingRequester {

  private static final Logger LOG = LoggerFactory.getLogger(LambdaOffloadingRequester.class.getName());

  private final AtomicInteger requestId = new AtomicInteger();
  private final String address;
  private final int port;
  private final AWSLambdaAsync awsLambda;

  public LambdaOffloadingRequester(final String address,
                                   final int port) {
    this.address = address;
    this.port = port;
    this.awsLambda = AWSLambdaAsyncClientBuilder.standard()
      .withRegion("ap-northeast-1").withClientConfiguration(
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
    awsLambda.invokeAsync(request);
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
