/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.nemo.runtime.master;

import com.amazonaws.regions.Regions;
import com.amazonaws.services.lambda.AWSLambda;
import com.amazonaws.services.lambda.AWSLambdaClientBuilder;
import com.amazonaws.services.lambda.model.InvokeRequest;
import com.amazonaws.services.lambda.model.InvokeResult;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.*;
import io.netty.channel.group.ChannelGroup;
import io.netty.channel.group.DefaultChannelGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.util.concurrent.DefaultThreadFactory;
import io.netty.util.concurrent.GlobalEventExecutor;
import org.apache.nemo.runtime.master.resource.LambdaExecutorRepresenter;
import org.apache.nemo.runtime.master.resource.LambdaInboundHandler;
import org.apache.nemo.runtime.master.resource.NettyChannelInitializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.*;

/**
 * Invoked by runtime master, with utilities for the LambdaExecutor to communicate
 * Set up netty server for LambdaExecutors to connect to.
 * Invoke LambdaExecutor with public addr and port.
 */
public final class LambdaMaster {

  private static final Logger LOG = LoggerFactory.getLogger(LambdaMaster.class.getName());
  private static final int SERVER_BOSS_NUM_THREADS = 3;
  private static final int SERVER_WORKER_NUM_THREADS = 10;
  private static final String CLASS_NAME = LambdaMaster.class.getName();

  private EventLoopGroup serverBossGroup;
  private EventLoopGroup serverWorkerGroup;
  private Channel acceptor;
  // A container object keeping track of all existing channels
  private final ChannelGroup serverChannelGroup = new DefaultChannelGroup(GlobalEventExecutor.INSTANCE);

  private static final int PORT = 9999;
  private String localAddress;
  private String publicAddress;

  private final Regions region;
  private final AWSLambda client;
  private static final String LAMBDA_FUNTIONNAME = "lambda-dev-executor";

  public static LambdaExecutorRepresenter lambdaExecutorRepresenter;

  @Inject
  public LambdaMaster() {
    this.region = Regions.fromName("ap-northeast-1");
    this.client = AWSLambdaClientBuilder.standard().withRegion(this.region).build();
  }

  /**
   * Invoke LambdaExecutor, which connects to the netty channel.
   */
  public void invokeExecutor() {
     InvokeRequest request = new InvokeRequest()
       .withFunctionName(this.LAMBDA_FUNTIONNAME)
       .withInvocationType("Event").withLogType("Tail").withClientContext("Lambda Executor")
       .withPayload(String.format("{\"address\":\"%s\", \"port\": %d}",
         this.publicAddress, this.PORT));

     InvokeResult response = client.invoke(request);
  }

  /**
   * Reef calls this function periodically to see if our job has finished.
   * Stub function now waiting for execution result from LambdaExecutor
   * TODO #407: LambdaHandler for single-stage execution
   * @return true then NemoDriver will be shutdown.
   */
  public boolean isCompleted() {
    return false;
  }

  /**
   * serverChannelGroup is passed to LambdaInboundHandler, adding channels when connected.
   * For now there is only one channel in the channel group -- since we only one LambdaExecutor.
   * @return channel.
   */
  public Channel getExecutorChannel() {
    Channel ch = null;
    while(ch == null) {
      for (Channel cn : this.serverChannelGroup) {
        ch = cn;
      }
      try {
      Thread.sleep(100);
      } catch (Exception e) {
        e.printStackTrace();
      }
    }
    return ch;
  }

  public void setNetty() {
    LOG.info("##### Set up netty server #####");
    this.serverBossGroup = new NioEventLoopGroup(SERVER_BOSS_NUM_THREADS,
      new DefaultThreadFactory(CLASS_NAME + "SourceServerBoss"));
    this.serverWorkerGroup = new NioEventLoopGroup(SERVER_WORKER_NUM_THREADS,
      new DefaultThreadFactory(CLASS_NAME + "SourceServerWorker"));

    final ServerBootstrap serverBootstrap = new ServerBootstrap();
    ChannelInboundHandlerAdapter channelInboundHandlerAdapter =
      new LambdaInboundHandler(this.serverChannelGroup, this);

    serverBootstrap.group(this.serverBossGroup, this.serverWorkerGroup)
      .channel(NioServerSocketChannel.class)
      .childHandler(new NettyChannelInitializer(channelInboundHandlerAdapter))
      .option(ChannelOption.SO_BACKLOG, 128)
      .option(ChannelOption.SO_REUSEADDR, true)
      .childOption(ChannelOption.SO_KEEPALIVE, true);
    System.out.println("##### Server Bootstrap set #####");

    this.publicAddress = this.getPublicIP();
    try {
      this.localAddress = this.getLocalHostLANAddress().getHostAddress();
    } catch (UnknownHostException e) {
      e.printStackTrace();
      throw new RuntimeException(e);
    }

    try {
      this.acceptor = serverBootstrap.bind(
        new InetSocketAddress(localAddress, this.PORT)).sync().channel();
      LOG.info("Server address: {}, Assigned server port = {}", localAddress, this.PORT);
    } catch (Exception e) {
      e.printStackTrace();
    }

    LOG.info("Public address: {}, localAddress: {}, port: {}", publicAddress, localAddress, this.PORT);
    LOG.info("Acceptor open: {}, active: {}", acceptor.isOpen(), acceptor.isActive());
  }

  /**
   * Register LambdaExecutorRepresenter to LambdaMaster.
   * LambdaMaster forward received LambdaEvent for representer to handle.
   * TODO #415: Multiple LambdaExecutor dispatching
   * LambdaMaster controls multiple executors through representer of the executor.
   */
  public void setRepresenter(LambdaExecutorRepresenter lambdaExecutorRepresenter) {
    this.lambdaExecutorRepresenter = lambdaExecutorRepresenter;
  }

  private String getPublicIP() {
    final URL whatismyip;
    try {
      whatismyip = new URL("http://checkip.amazonaws.com");
    } catch (MalformedURLException e) {
      e.printStackTrace();
      throw new RuntimeException(e);
    }

    final BufferedReader in;
    try {
      in = new BufferedReader(new InputStreamReader(
        whatismyip.openStream()));
    } catch (IOException e) {
      e.printStackTrace();
      throw new RuntimeException(e);
    }

    String ip = null; //you get the IP as a String
    try {
      ip = in.readLine();
    } catch (IOException e) {
      e.printStackTrace();
      throw new RuntimeException(e);
    }
    return ip;
  }

  private InetAddress getLocalHostLANAddress() throws UnknownHostException {
    try {
      InetAddress candidateAddress = null;
      // Iterate all NICs (network interface cards)...

      if (candidateAddress != null) {
        // We did not find a site-local address, but we found some other non-loopback address.
        // Server might have a non-site-local address assigned to its NIC (or it might be running
        // IPv6 which deprecates the "site-local" concept).
        // Return this non-loopback candidate address...
        return candidateAddress;
      }
      // At this point, we did not find a non-loopback address.
      // Fall back to returning whatever InetAddress.getLocalHost() returns...
      InetAddress jdkSuppliedAddress = InetAddress.getLocalHost();
      if (jdkSuppliedAddress == null) {
        throw new UnknownHostException("The JDK InetAddress.getLocalHost() method unexpectedly returned null.");
      }
      return jdkSuppliedAddress;
    } catch (Exception e) {
      UnknownHostException unknownHostException = new UnknownHostException("Failed to determine LAN address: " + e);
      unknownHostException.initCause(e);
      throw unknownHostException;
    }
  }
}
