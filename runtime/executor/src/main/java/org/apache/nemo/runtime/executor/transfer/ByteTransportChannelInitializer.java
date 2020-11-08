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
package org.apache.nemo.runtime.executor.transfer;

import io.netty.channel.ChannelInitializer;
import io.netty.channel.socket.SocketChannel;
import org.apache.nemo.conf.JobConf;
import org.apache.nemo.runtime.executor.data.BlockManagerWorker;
import org.apache.nemo.runtime.executor.data.PipeManagerWorker;
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

  private final InjectionFuture<PipeManagerWorker> pipeManagerWorker;
  private final InjectionFuture<BlockManagerWorker> blockManagerWorker;
  private final InjectionFuture<ByteTransfer> byteTransfer;
  private final InjectionFuture<ByteTransport> byteTransport;
  private final ControlFrameEncoder controlFrameEncoder;
  private final DataFrameEncoder dataFrameEncoder;
  private final String localExecutorId;

  /**
   * Creates a netty channel initializer.
   *
   * @param pipeManagerWorker   provides handler for new contexts by remote executors
   * @param blockManagerWorker  provides handler for new contexts by remote executors
   * @param byteTransfer        provides channel caching
   * @param byteTransport       provides {@link io.netty.channel.group.ChannelGroup}
   * @param controlFrameEncoder encodes control frames
   * @param dataFrameEncoder    encodes data frames
   * @param localExecutorId     the id of this executor
   */
  @Inject
  private ByteTransportChannelInitializer(final InjectionFuture<PipeManagerWorker> pipeManagerWorker,
                                          final InjectionFuture<BlockManagerWorker> blockManagerWorker,
                                          final InjectionFuture<ByteTransfer> byteTransfer,
                                          final InjectionFuture<ByteTransport> byteTransport,
                                          final ControlFrameEncoder controlFrameEncoder,
                                          final DataFrameEncoder dataFrameEncoder,
                                          @Parameter(JobConf.ExecutorId.class) final String localExecutorId) {
    this.pipeManagerWorker = pipeManagerWorker;
    this.blockManagerWorker = blockManagerWorker;
    this.byteTransfer = byteTransfer;
    this.byteTransport = byteTransport;
    this.controlFrameEncoder = controlFrameEncoder;
    this.dataFrameEncoder = dataFrameEncoder;
    this.localExecutorId = localExecutorId;
  }

  @Override
  protected void initChannel(final SocketChannel ch) {
    final ContextManager contextManager = new ContextManager(pipeManagerWorker.get(), blockManagerWorker.get(),
      byteTransfer.get(), byteTransport.get().getChannelGroup(), localExecutorId, ch);
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
