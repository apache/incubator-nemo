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

import io.netty.channel.ChannelInitializer;
import io.netty.channel.group.ChannelGroup;
import io.netty.channel.socket.SocketChannel;
import org.apache.nemo.common.Pair;
import org.apache.nemo.runtime.executor.common.OutputWriterFlusher;
import org.apache.nemo.runtime.executor.common.datatransfer.*;

import java.util.Map;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
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
public final class LambdaByteTransportChannelInitializer extends ChannelInitializer<SocketChannel> {

  private final ControlFrameEncoder controlFrameEncoder;
  private final DataFrameEncoder dataFrameEncoder;
  private final String localExecutorId;
  private final ConcurrentMap<SocketChannel, Boolean> channels;
  private final ChannelGroup channelGroup;
  private final AckScheduledService ackScheduledService;
  private final Map<TransferKey, Integer> taskTransferIndexMap;
  private final ExecutorService channelExecutorService;
  private RelayServerClient relayServerClient;
  final ConcurrentMap<Integer, ByteInputContext> inputContextMap;
  final ConcurrentMap<Integer, ByteOutputContext> outputContextMap;
  private ByteTransfer byteTransfer;
  private final OutputWriterFlusher outputWriterFlusher;

  public LambdaByteTransportChannelInitializer(final ChannelGroup channelGroup,
                                               final ControlFrameEncoder controlFrameEncoder,
                                               final DataFrameEncoder dataFrameEncoder,
                                               final ConcurrentMap<SocketChannel, Boolean> channels,
                                               final String localExecutorId,
                                               final AckScheduledService ackScheduledService,
                                               final Map<TransferKey, Integer> taskTransferIndexMap,
                                               final ConcurrentMap<Integer, ByteInputContext> inputContextMap,
                                               final ConcurrentMap<Integer, ByteOutputContext> outputContextMap,
                                               final OutputWriterFlusher outputWriterFlusher) {
    this.channelGroup = channelGroup;
    this.controlFrameEncoder = controlFrameEncoder;
    this.dataFrameEncoder = dataFrameEncoder;
    this.localExecutorId = localExecutorId;
    this.channels = channels;
    this.ackScheduledService = ackScheduledService;
    this.taskTransferIndexMap = taskTransferIndexMap;
    this.channelExecutorService = Executors.newCachedThreadPool();
    this.inputContextMap = inputContextMap;
    this.outputContextMap = outputContextMap;
    this.outputWriterFlusher = outputWriterFlusher;
  }

  public void setByteTransfer(final ByteTransfer bt) {
    byteTransfer = bt;
  }

  public void setRelayServerClient(final RelayServerClient client) {
    relayServerClient = client;
  }

  @Override
  protected void initChannel(final SocketChannel ch) {

    final ContextManager contextManager = new LambdaContextManager(
      channelExecutorService,
      inputContextMap,
      outputContextMap,
      channelGroup, localExecutorId, ch, ackScheduledService, taskTransferIndexMap,
      false, relayServerClient, byteTransfer, outputWriterFlusher);

    System.out.println("Init channel " + ch);

    channels.put(ch, true);

    ch.pipeline()
        // outbound
        .addLast(controlFrameEncoder)
        .addLast(dataFrameEncoder)
        // inbound
        .addLast(new FrameDecoder(contextManager))
        .addLast(contextManager);
  }

}
