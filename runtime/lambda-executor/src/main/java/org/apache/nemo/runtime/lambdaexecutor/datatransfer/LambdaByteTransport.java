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
package org.apache.nemo.runtime.lambdaexecutor.datatransfer;

import io.netty.bootstrap.Bootstrap;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.group.ChannelGroup;
import io.netty.channel.group.ChannelGroupFuture;
import io.netty.channel.group.DefaultChannelGroup;
import io.netty.util.concurrent.DefaultThreadFactory;
import io.netty.util.concurrent.Future;

import io.netty.util.concurrent.GlobalEventExecutor;
import org.apache.nemo.conf.JobConf;
import org.apache.nemo.runtime.executor.common.ByteTransport;
import org.apache.nemo.runtime.executor.common.ByteTransportChannelInitializer;
import org.apache.nemo.runtime.message.NemoNameResolver;
import org.apache.nemo.runtime.message.PersistentConnectionToMasterMap;
import org.apache.reef.tang.annotations.Parameter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import java.net.InetSocketAddress;
import java.util.Map;

/**
 * Bootstraps the server and connects to other servers on demand.
 */
public final class LambdaByteTransport implements ByteTransport {//implements AutoCloseable {

  private static final Logger LOG = LoggerFactory.getLogger(LambdaByteTransport.class);
  private static final String CLIENT = "byte:client";

  private final ChannelGroup channelGroup = new DefaultChannelGroup(GlobalEventExecutor.INSTANCE);
  private final EventLoopGroup clientGroup;
  private final Bootstrap clientBootstrap;
  private final NemoNameResolver nameResolver;
  private final String localExecutorId;

  @Inject
  private LambdaByteTransport(
    final NemoNameResolver nameResolver,
    @Parameter(JobConf.ExecutorId.class) final String localExecutorId,
    final ByteTransportChannelInitializer channelInitializer) {

    final NativeChannelImplementationSelector channelImplSelector =
      new NativeChannelImplementationSelector();

    this.nameResolver = nameResolver;
    this.localExecutorId = localExecutorId;

    this.clientGroup = channelImplSelector.newEventLoopGroup(10,
      new DefaultThreadFactory(CLIENT));

    this.clientBootstrap = new Bootstrap()
        .group(clientGroup)
        .channel(channelImplSelector.getChannelClass())
        .handler(channelInitializer)
        .option(ChannelOption.SO_REUSEADDR, true);
  }


  @Override
  public void close() {
    channelGroup.flush();
    final ChannelGroupFuture channelGroupCloseFuture = channelGroup.close();
    final Future clientGroupCloseFuture = clientGroup.shutdownGracefully();
    channelGroupCloseFuture.awaitUninterruptibly();
    clientGroupCloseFuture.awaitUninterruptibly();
  }

  @Override
  public InetSocketAddress getAndPutInetAddress(final String remoteExecutorId) {
    final InetSocketAddress address;
    try {
      address = nameResolver.lookup(remoteExecutorId + "-Public");
      LOG.info("Address of {}: {}", remoteExecutorId, address);
      //executorAddressMap.put(remoteExecutorId, address);
      return address;
    } catch (final Exception e) {
      LOG.error(String.format("Cannot lookup DefaultByteTransportImpl listening address of %s", remoteExecutorId), e);
      throw new RuntimeException(e);
    }
  }

  @Override
  public ChannelFuture connectTo(final String remoteExecutorId) {

    if (remoteExecutorId.contains("Lambda")) {
      throw new RuntimeException("Should not connect to " + remoteExecutorId);
    }

    final InetSocketAddress address = getAndPutInetAddress(remoteExecutorId);
    final ChannelFuture connectFuture = clientBootstrap.connect(address);
    connectFuture.addListener(future -> {
      if (future.isSuccess()) {
        // Succeed to connect
        LOG.debug("Connected to {}", remoteExecutorId);
        return;
      }
      // Failed to connect (Not logging the cause here, which is not very useful)
      LOG.error("Failed to connect to {}", remoteExecutorId);
    });
    return connectFuture;
  }

  @Override
  public ChannelGroup getChannelGroup() {
    return channelGroup;
  }
}
