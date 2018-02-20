/*
 * Copyright (C) 2018 Seoul National University
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
package edu.snu.nemo.runtime.executor.bytetransfer;

import edu.snu.nemo.conf.JobConf;
import edu.snu.nemo.runtime.executor.data.BlockManagerWorker;
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
