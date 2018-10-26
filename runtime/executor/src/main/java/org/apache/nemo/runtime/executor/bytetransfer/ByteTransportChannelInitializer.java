package org.apache.nemo.runtime.executor.bytetransfer;

import org.apache.nemo.conf.JobConf;
import org.apache.nemo.runtime.executor.data.BlockManagerWorker;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.socket.SocketChannel;
import org.apache.reef.tang.InjectionFuture;
import org.apache.reef.tang.annotations.Parameter;

import javax.inject.Inject;

/**
 * Sets up {@link io.netty.channel.ChannelPipeline} for {@link ByteTransport}.
 *
 * <h3>Inbound pipeline:</h3>
 * <pre>
 * {@literal
 *                                    +----------------+
 *                        += Control =| ContextManager | => A new ByteTransferContext
 *      +--------------+  |           +----------------+
 *   => | FrameDecoder | =|
 *      +--------------+  |
 *                        += Data ==== (ContextManager) ==> Add data to an existing ByteInputContext
 * }
 * </pre>
 *
 * <h3>Outbound pipeline:</h3>
 * <pre>
 * {@literal
 *      +---------------------+
 *   <= | ControlFrameEncoder | <== A new ByteTransferContext
 *      +---------------------+
 *      +------------------+
 *   <= | DataFrameEncoder | <==== ByteBuf ==== Writing bytes to ByteOutputStream
 *      +------------------+
 *      +------------------+
 *   <= | DataFrameEncoder | <== FileRegion === A FileArea added to ByteOutputStream
 *      +------------------+
 * }
 * </pre>
 */
final class ByteTransportChannelInitializer extends ChannelInitializer<SocketChannel> {

  private final InjectionFuture<BlockManagerWorker> blockManagerWorker;
  private final InjectionFuture<ByteTransfer> byteTransfer;
  private final InjectionFuture<ByteTransport> byteTransport;
  private final ControlFrameEncoder controlFrameEncoder;
  private final DataFrameEncoder dataFrameEncoder;
  private final String localExecutorId;

  /**
   * Creates a netty channel initializer.
   *
   * @param blockManagerWorker  provides handler for new contexts by remote executors
   * @param byteTransfer        provides channel caching
   * @param byteTransport       provides {@link io.netty.channel.group.ChannelGroup}
   * @param controlFrameEncoder encodes control frames
   * @param dataFrameEncoder    encodes data frames
   * @param localExecutorId     the id of this executor
   */
  @Inject
  private ByteTransportChannelInitializer(final InjectionFuture<BlockManagerWorker> blockManagerWorker,
                                          final InjectionFuture<ByteTransfer> byteTransfer,
                                          final InjectionFuture<ByteTransport> byteTransport,
                                          final ControlFrameEncoder controlFrameEncoder,
                                          final DataFrameEncoder dataFrameEncoder,
                                          @Parameter(JobConf.ExecutorId.class) final String localExecutorId) {
    this.blockManagerWorker = blockManagerWorker;
    this.byteTransfer = byteTransfer;
    this.byteTransport = byteTransport;
    this.controlFrameEncoder = controlFrameEncoder;
    this.dataFrameEncoder = dataFrameEncoder;
    this.localExecutorId = localExecutorId;
  }

  @Override
  protected void initChannel(final SocketChannel ch) {
    final ContextManager contextManager = new ContextManager(blockManagerWorker.get(), byteTransfer.get(),
        byteTransport.get().getChannelGroup(), localExecutorId, ch);
    ch.pipeline()
        // inbound
        .addLast(new FrameDecoder(contextManager))
        // outbound
        .addLast(controlFrameEncoder)
        .addLast(dataFrameEncoder)
        // inbound
        .addLast(contextManager);
  }
}
