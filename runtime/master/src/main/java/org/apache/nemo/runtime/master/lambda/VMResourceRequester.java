package org.apache.nemo.runtime.master.lambda;


import com.amazonaws.ClientConfiguration;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.services.lambda.AWSLambdaAsync;
import com.amazonaws.services.lambda.AWSLambdaAsyncClientBuilder;
import com.amazonaws.services.lambda.model.InvokeRequest;
import io.netty.bootstrap.Bootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.util.concurrent.DefaultThreadFactory;
import org.apache.nemo.conf.EvalConf;
import org.apache.nemo.offloading.client.AWSUtils;
import org.apache.nemo.offloading.common.EventHandler;
import org.apache.nemo.offloading.common.NettyChannelInitializer;
import org.apache.nemo.offloading.common.NettyLambdaInboundHandler;
import org.apache.nemo.offloading.common.OffloadingMasterEvent;
import org.apache.nemo.runtime.master.vmscaling.VMScalingAddresses;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import java.net.InetSocketAddress;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;

import static org.apache.nemo.offloading.common.Constants.VM_WORKER_PORT;

public final class VMResourceRequester implements LambdaContainerRequester {

  private static final Logger LOG = LoggerFactory.getLogger(VMResourceRequester.class.getName());

  private final AtomicInteger numVmCreated = new AtomicInteger(0);

  private final Bootstrap clientBootstrap;
  private final EventLoopGroup clientWorkerGroup;
  private final ConcurrentMap<Channel, EventHandler<OffloadingMasterEvent>> map;
  private final VMScalingAddresses vmScalingAddresses;

  @Inject
  private VMResourceRequester(final EvalConf evalConf,
                              final VMScalingAddresses vmScalingAddresses) {
    final ProfileCredentialsProvider provider = new ProfileCredentialsProvider(evalConf.awsProfileName);
    this.vmScalingAddresses = vmScalingAddresses;
    this.clientWorkerGroup = new NioEventLoopGroup(30,
      new DefaultThreadFactory("hello" + "-ClientWorker"));

    // Registration
    this.clientBootstrap = new Bootstrap();
    this.map = new ConcurrentHashMap<>();
    this.clientBootstrap.group(clientWorkerGroup)
      .channel(NioSocketChannel.class)
      .handler(new NettyChannelInitializer(new NettyLambdaInboundHandler(map)))
      .option(ChannelOption.SO_REUSEADDR, true)
      .option(ChannelOption.SO_KEEPALIVE, true);
  }

  @Override
  public synchronized LambdaActivator createRequest(String address, int port,
                                                    String containerType,
                                                    int capacity,
                                                    int slot,
                                                    int memory) {
    final int index = numVmCreated.getAndIncrement();
    final String vmAddress = vmScalingAddresses.vmAddresses.get(index);
    ChannelFuture channelFuture;
    final long waitingTime = 1000;

    while (true) {
      final long st = System.currentTimeMillis();
      LOG.info("Connection to VM-{} {}/{}", index + 1, vmAddress, VM_WORKER_PORT);
      channelFuture = clientBootstrap.connect(new InetSocketAddress(vmAddress, VM_WORKER_PORT));
      channelFuture.awaitUninterruptibly(waitingTime);
      assert channelFuture.isDone();
      if (!channelFuture.isSuccess()) {
        LOG.warn("A connection failed for " + vmAddress + "  waiting...");
        final long elapsedTime = System.currentTimeMillis() - st;
        try {
          Thread.sleep(Math.max(1, waitingTime - elapsedTime));
        } catch (InterruptedException e) {
          e.printStackTrace();
        }
      } else {
        break;
      }
    }

    final Channel openChannel = channelFuture.channel();
    LOG.info("Open channel for VM-{}: {}", index+1, openChannel);

    // send handshake
    final byte[] bytes = String.format("{\"address\":\"%s\", \"port\": %d, \"requestId\": %d}",
      address, port, (index + 1)).getBytes();
    openChannel.writeAndFlush(new OffloadingMasterEvent(OffloadingMasterEvent.Type.SEND_ADDRESS, bytes, bytes.length));

    LOG.info("Add channel: {}, address: {}", openChannel, openChannel.remoteAddress());

    final String executorId = "VM-" + (index + 1);

    LOG.info("Invoke create VM request for requestId: {}/{}",
      (index + 1) , executorId);
    // final Future<InvokeResult> future = awsLambda.invokeAsync(request);

    return new LambdaActivator() {

      @Override
      public void activate() {
      }

      @Override
      public int getRequestId() {
        return index + 1;
      }

      @Override
      public String getExecutorId() {
        return executorId;
      }
    };
  }
}
