package org.apache.nemo.runtime.executor.bytetransfer;

import io.netty.channel.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.concurrent.ThreadSafe;
import javax.inject.Inject;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * Manages channels and exposes interface for {@link org.apache.nemo.runtime.executor.data.BlockManagerWorker}.
 */
@ThreadSafe
public final class ByteTransfer {

  private static final Logger LOG = LoggerFactory.getLogger(ByteTransfer.class);

  private final ByteTransport byteTransport;
  private final ConcurrentMap<String, ChannelFuture> executorIdToChannelFutureMap = new ConcurrentHashMap<>();

  /**
   * Creates a byte transfer.
   * @param byteTransport provides channels to other executors
   */
  @Inject
  private ByteTransfer(final ByteTransport byteTransport) {
    this.byteTransport = byteTransport;
  }

  /**
   * Initiate a transfer context to receive data.
   * @param executorId        the id of the remote executor
   * @param contextDescriptor user-provided descriptor for the new context
   * @return a {@link ByteInputContext} from which the received data can be read
   */
  public CompletableFuture<ByteInputContext> newInputContext(final String executorId,
                                                             final byte[] contextDescriptor) {
    return connectTo(executorId).thenApply(manager -> manager.newInputContext(executorId, contextDescriptor));
  }

  /**
   * Initiate a transfer context to send data.
   * @param executorId         the id of the remote executor
   * @param contextDescriptor  user-provided descriptor for the new context
   * @return a {@link ByteOutputContext} to which data can be written
   */
  public CompletableFuture<ByteOutputContext> newOutputContext(final String executorId,
                                                               final byte[] contextDescriptor) {
    return connectTo(executorId).thenApply(manager -> manager.newOutputContext(executorId, contextDescriptor));
  }

  /**
   * @param remoteExecutorId id of the remote executor
   * @return {@link ContextManager} for the channel to the specified executor
   */
  private CompletableFuture<ContextManager> connectTo(final String remoteExecutorId) {
    final CompletableFuture<ContextManager> completableFuture = new CompletableFuture<>();
    final ChannelFuture channelFuture;
    try {
      channelFuture = executorIdToChannelFutureMap.compute(remoteExecutorId, (executorId, cachedChannelFuture) -> {
        if (cachedChannelFuture != null
            && (cachedChannelFuture.channel().isOpen() || cachedChannelFuture.channel().isActive())) {
          return cachedChannelFuture;
        } else {
          final ChannelFuture future = byteTransport.connectTo(executorId);
          future.channel().closeFuture().addListener(f -> executorIdToChannelFutureMap.remove(executorId, future));
          return future;
        }
      });
    } catch (final RuntimeException e) {
      completableFuture.completeExceptionally(e);
      return completableFuture;
    }
    channelFuture.addListener(future -> {
      if (future.isSuccess()) {
        completableFuture.complete(channelFuture.channel().pipeline().get(ContextManager.class));
      } else {
        executorIdToChannelFutureMap.remove(remoteExecutorId, channelFuture);
        completableFuture.completeExceptionally(future.cause());
      }
    });
    return completableFuture;
  }

  /**
   * Called when a remote executor initiates new transfer context.
   * @param remoteExecutorId  id of the remote executor
   * @param channel           the corresponding {@link Channel}.
   */
  void onNewContextByRemoteExecutor(final String remoteExecutorId, final Channel channel) {
    executorIdToChannelFutureMap.compute(remoteExecutorId, (executorId, cachedChannelFuture) -> {
      if (cachedChannelFuture == null) {
        LOG.debug("Remote {}({}) connected to this executor", new Object[]{executorId, channel.remoteAddress()});
        return channel.newSucceededFuture();
      } else if (channel == cachedChannelFuture.channel()) {
        return cachedChannelFuture;
      } else {
        LOG.warn("Duplicate channel for remote {}({}) and this executor",
            new Object[]{executorId, channel.remoteAddress()});
        return channel.newSucceededFuture();
      }
    });
  }
}
