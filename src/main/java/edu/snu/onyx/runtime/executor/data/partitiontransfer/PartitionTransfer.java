/*
 * Copyright (C) 2017 Seoul National University
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package edu.snu.onyx.runtime.executor.data.partitiontransfer;

import edu.snu.onyx.client.JobConf;
import edu.snu.onyx.runtime.executor.data.HashRange;
import edu.snu.onyx.runtime.executor.data.PartitionManagerWorker;
import edu.snu.onyx.runtime.executor.data.stores.PartitionStore;
import io.netty.channel.*;
import io.netty.channel.group.ChannelGroup;
import io.netty.channel.group.DefaultChannelGroup;
import io.netty.util.concurrent.DefaultThreadFactory;
import io.netty.util.concurrent.GlobalEventExecutor;
import org.apache.reef.tang.InjectionFuture;
import org.apache.reef.tang.annotations.Parameter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.function.Consumer;

/**
 * Manages channels and exposes an interface for {@link PartitionManagerWorker}.
 */
@ChannelHandler.Sharable
public final class PartitionTransfer extends SimpleChannelInboundHandler<PartitionStream> {

  private static final Logger LOG = LoggerFactory.getLogger(PartitionTransfer.class);
  private static final String INBOUND = "partition:inbound";
  private static final String OUTBOUND = "partition:outbound";

  private final InjectionFuture<PartitionManagerWorker> partitionManagerWorker;
  private final PartitionTransport partitionTransport;
  private final String localExecutorId;
  private final int bufferSize;

  private final ConcurrentMap<String, ChannelFuture> executorIdToChannelFutureMap = new ConcurrentHashMap<>();
  private final ConcurrentMap<Channel, String> channelToExecutorIdMap = new ConcurrentHashMap<>();
  private final ChannelGroup channelGroup = new DefaultChannelGroup(GlobalEventExecutor.INSTANCE);
  private final ExecutorService inboundExecutorService;
  private final ExecutorService outboundExecutorService;

  /**
   * Creates a partition transfer and registers this transfer to the name server.
   *
   * @param partitionManagerWorker  provides {@link edu.snu.onyx.common.coder.Coder}s
   * @param partitionTransport      provides {@link io.netty.channel.Channel}
   * @param localExecutorId         the id of this executor
   * @param inboundThreads          the number of threads in thread pool for inbound partition transfer
   * @param outboundThreads         the number of threads in thread pool for outbound partition transfer
   * @param bufferSize              the size of outbound buffers
   */
  @Inject
  private PartitionTransfer(
      final InjectionFuture<PartitionManagerWorker> partitionManagerWorker,
      final PartitionTransport partitionTransport,
      @Parameter(JobConf.ExecutorId.class) final String localExecutorId,
      @Parameter(JobConf.PartitionTransferInboundNumThreads.class) final int inboundThreads,
      @Parameter(JobConf.PartitionTransferOutboundNumThreads.class) final int outboundThreads,
      @Parameter(JobConf.PartitionTransferOutboundBufferSize.class) final int bufferSize) {

    this.partitionManagerWorker = partitionManagerWorker;
    this.partitionTransport = partitionTransport;
    this.localExecutorId = localExecutorId;
    this.bufferSize = bufferSize;

    // Inbound thread pool can be easily saturated with multiple data transfers with the encodePartialPartition option
    // enabled. We may consider other solutions than using fixed thread pool.
    this.inboundExecutorService = Executors.newFixedThreadPool(inboundThreads, new DefaultThreadFactory(INBOUND));
    this.outboundExecutorService = Executors.newFixedThreadPool(outboundThreads, new DefaultThreadFactory(OUTBOUND));
  }

  /**
   * Initiate a pull-based partition transfer.
   *
   * @param executorId              the id of the source executor
   * @param encodePartialPartition  whether the sender should start encoding even though the whole partition
   *                                has not been written yet
   * @param partitionStore          the partition store
   * @param partitionId             the id of the partition to transfer
   * @param runtimeEdgeId           the runtime edge id
   * @param hashRange               the hash range
   * @return a {@link PartitionInputStream} from which the received
   *         {@link edu.snu.onyx.compiler.ir.Element}s can be read
   */
  public PartitionInputStream initiatePull(final String executorId,
                                           final boolean encodePartialPartition,
                                           final Class<? extends PartitionStore> partitionStore,
                                           final String partitionId,
                                           final String runtimeEdgeId,
                                           final HashRange hashRange) {
    final PartitionInputStream stream = new PartitionInputStream(executorId, encodePartialPartition,
        Optional.of(partitionStore), partitionId, runtimeEdgeId, hashRange);
    stream.setCoderAndExecutorService(partitionManagerWorker.get().getCoder(runtimeEdgeId), inboundExecutorService);
    write(executorId, stream, stream::onExceptionCaught);
    return stream;
  }

  /**
   * Initiate a push-based partition transfer.
   *
   * @param executorId              the id of the destination executor
   * @param encodePartialPartition  whether to start encoding even though the whole partition has not been written yet
   * @param partitionId             the id of the partition to transfer
   * @param runtimeEdgeId           the runtime edge id
   * @param hashRange               the hash range
   * @return a {@link PartitionOutputStream} to which {@link edu.snu.onyx.compiler.ir.Element}s can be written
   */
  public PartitionOutputStream initiatePush(final String executorId,
                                            final boolean encodePartialPartition,
                                            final String partitionId,
                                            final String runtimeEdgeId,
                                            final HashRange hashRange) {
    final PartitionOutputStream stream = new PartitionOutputStream(executorId, encodePartialPartition, Optional.empty(),
        partitionId, runtimeEdgeId, hashRange);
    stream.setCoderAndExecutorServiceAndBufferSize(partitionManagerWorker.get().getCoder(runtimeEdgeId),
        outboundExecutorService, bufferSize);
    write(executorId, stream, stream::onExceptionCaught);
    return stream;
  }

  /**
   * Gets a {@link ChannelFuture} for connecting to the {@link PartitionTransport} server of the specified executor.
   *
   * @param remoteExecutorId  the id of the remote executor
   * @param stream            the partition stream object to write
   * @param onError           the {@link Consumer} to be invoked on an error during setting up a channel
   *                          or writing to the channel
   */
  private void write(final String remoteExecutorId, final PartitionStream stream, final Consumer<Throwable> onError) {
    final ChannelFuture channelFuture = executorIdToChannelFutureMap.computeIfAbsent(remoteExecutorId, executorId -> {
      // No cached channel found
      final ChannelFuture connectFuture = partitionTransport.connectTo(executorId, onError);
      connectFuture.addListener(future -> {
        if (future.isSuccess()) {
          // Succeed to connect
          LOG.debug("Local {} connected to remote {}", localExecutorId, executorId);
          return;
        }
        // Failed to connect
        if (future.cause() == null) {
          LOG.error("Failed to connect to {}", remoteExecutorId);
        } else {
          LOG.error(String.format("Failed to connect to %s", remoteExecutorId), future.cause());
        }
      });
      return connectFuture;
    });
    channelFuture.addListener(future -> {
      if (future.isSuccess()) {
        channelToExecutorIdMap.put(channelFuture.channel(), remoteExecutorId);
        channelFuture.channel().writeAndFlush(stream)
            .addListener(new ControlMessageWriteFutureListener(channelFuture, remoteExecutorId, onError));
        return;
      }
      executorIdToChannelFutureMap.remove(remoteExecutorId, channelFuture);
      if (future.cause() != null) {
        onError.accept(future.cause());
      }
    });
  }

  @Override
  protected void channelRead0(final ChannelHandlerContext ctx, final PartitionStream stream) {
    final Channel channel = ctx.channel();
    final String remoteExecutorId = stream.getRemoteExecutorId();
    channelToExecutorIdMap.put(channel, remoteExecutorId);
    executorIdToChannelFutureMap.compute(remoteExecutorId, (executorId, cachedChannelFuture) -> {
      if (cachedChannelFuture == null) {
        LOG.debug("Remote {}({}) connected to {}({})",
            new Object[]{executorId, channel.remoteAddress(), localExecutorId, channel.localAddress()});
        return channel.newSucceededFuture();
      } else if (channel == cachedChannelFuture.channel()) {
        return cachedChannelFuture;
      } else {
        LOG.warn("Remote {}({}) connected to {}({}) while a channel between two executors is already cached",
            new Object[]{executorId, channel.remoteAddress(), localExecutorId, channel.localAddress()});
        return channel.newSucceededFuture();
      }
    });

    // process the inbound control message
    if (stream instanceof PartitionInputStream) {
      onPushNotification((PartitionInputStream) stream);
    } else {
      onPullRequest((PartitionOutputStream) stream);
    }
  }

  /**
   * Respond to a new pull request.
   *
   * @param stream  {@link PartitionOutputStream}
   */
  private void onPullRequest(final PartitionOutputStream stream) {
    stream.setCoderAndExecutorServiceAndBufferSize(partitionManagerWorker.get().getCoder(stream.getRuntimeEdgeId()),
        outboundExecutorService, bufferSize);
    partitionManagerWorker.get().onPullRequest(stream);
  }

  /**
   * Respond to a new push notification.
   *
   * @param stream  {@link PartitionInputStream}
   */
  private void onPushNotification(final PartitionInputStream stream) {
    stream.setCoderAndExecutorService(partitionManagerWorker.get().getCoder(stream.getRuntimeEdgeId()),
        inboundExecutorService);
    partitionManagerWorker.get().onPushNotification(stream);
  }

  @Override
  public void channelActive(final ChannelHandlerContext ctx) {
    channelGroup.add(ctx.channel());
  }

  @Override
  public void channelInactive(final ChannelHandlerContext ctx) {
    final Channel channel = ctx.channel();
    channelGroup.remove(channel);
    final String remoteExecutorId = channelToExecutorIdMap.remove(channel);
    if (remoteExecutorId == null) {
      LOG.warn("An unidentified channel is now inactive (local: {}, remote: {})", channel.localAddress(),
          channel.remoteAddress());
    } else {
      executorIdToChannelFutureMap.computeIfPresent(remoteExecutorId, (executorId, cachedChannelFuture) -> {
        if (channel == cachedChannelFuture.channel()) {
          // remove it
          return null;
        } else {
          // leave unchanged
          return cachedChannelFuture;
        }
      });
      LOG.warn("A channel between local {}({}) and remote {}({}) is now inactive",
          new Object[]{localExecutorId, channel.localAddress(), remoteExecutorId, channel.remoteAddress()});
    }
  }

  @Override
  public void exceptionCaught(final ChannelHandlerContext ctx, final Throwable cause) {
    LOG.error(String.format("Exception caught in the channel with local address %s and remote address %s",
        ctx.channel().localAddress(), ctx.channel().remoteAddress()), cause);
    ctx.close();
  }

  /**
   * Gets the channel group.
   *
   * @return the channel group
   */
  ChannelGroup getChannelGroup() {
    return channelGroup;
  }

  /**
   * {@link ChannelFutureListener} for handling outbound exceptions on writing control messages.
   */
  private final class ControlMessageWriteFutureListener implements ChannelFutureListener {

    private final ChannelFuture channelFuture;
    private final String remoteExecutorId;
    private final Consumer<Throwable> onError;

    /**
     * Creates a {@link ControlMessageWriteFutureListener}.
     *
     * @param channelFuture     the channel future
     * @param remoteExecutorId  the id of the remote executor
     * @param onError           the {@link Consumer} to be invoked on an error during writing to the channel
     */
    private ControlMessageWriteFutureListener(final ChannelFuture channelFuture,
                                              final String remoteExecutorId,
                                              final Consumer<Throwable> onError) {
      this.channelFuture = channelFuture;
      this.remoteExecutorId = remoteExecutorId;
      this.onError = onError;
    }

    @Override
    public void operationComplete(final ChannelFuture future) {
      if (future.isSuccess()) {
        return;
      }
      // Remove the channel from channel cache if needed
      executorIdToChannelFutureMap.remove(remoteExecutorId, channelFuture);
      channelToExecutorIdMap.remove(channelFuture.channel());
      if (future.cause() == null) {
        LOG.error("Failed to write a control message from {} to {}", localExecutorId, remoteExecutorId);
      } else {
        onError.accept(future.cause());
        LOG.error(String.format("Failed to write a control message from %s to %s", localExecutorId,
            remoteExecutorId), future.cause());
      }
      channelFuture.channel().close();
    }
  }
}
