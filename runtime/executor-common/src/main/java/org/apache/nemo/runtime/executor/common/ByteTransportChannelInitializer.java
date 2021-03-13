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
package org.apache.nemo.runtime.executor.common;

import org.apache.nemo.conf.EvalConf;
import org.apache.nemo.conf.JobConf;
import org.apache.nemo.runtime.executor.common.datatransfer.*;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.socket.SocketChannel;
import org.apache.nemo.runtime.executor.common.datatransfer.PipeManagerWorker;
import org.apache.reef.tang.annotations.Parameter;

import javax.inject.Inject;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * Sets up {@link io.netty.channel.ChannelPipeline} for {@link }.
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
public final class ByteTransportChannelInitializer extends ChannelInitializer<SocketChannel> {

  private PipeManagerWorker pipeManagerWorker;
  private ByteTransport byteTransport;
  private final ControlFrameEncoder controlFrameEncoder;
  private final DataFrameEncoder dataFrameEncoder;
  private final String localExecutorId;


  private final ConcurrentMap<Integer, ByteInputContext> inputContexts = new ConcurrentHashMap<>();
  private final ConcurrentMap<Integer, ByteOutputContext> outputContexts = new ConcurrentHashMap<>();
  //private final ConcurrentMap<Integer, ByteInputContext> inputContextsInitiatedByRemote = new ConcurrentHashMap<>();
  //private final ConcurrentMap<Integer, ByteOutputContext> outputContextsInitiatedByRemote = new ConcurrentHashMap<>();

  private final OutputWriterFlusher outputWriterFlusher;
  private final LambdaChannelMap lambdaChannelMap;

  /**
   * Creates a netty channel initializer.
   *
   * @param controlFrameEncoder encodes control frames
   * @param dataFrameEncoder    encodes data frames
   * @param localExecutorId     the id of this executor
   */
  @Inject
  private ByteTransportChannelInitializer(final ControlFrameEncoder controlFrameEncoder,
                                          final DataFrameEncoder dataFrameEncoder,
                                          final LambdaChannelMap lambdaChannelMap,
                                          @Parameter(JobConf.ExecutorId.class) final String localExecutorId,
                                          @Parameter(EvalConf.FlushPeriod.class) final int flushPeriod) {

    this.controlFrameEncoder = controlFrameEncoder;
    this.lambdaChannelMap = lambdaChannelMap;
    this.dataFrameEncoder = dataFrameEncoder;
    this.localExecutorId = localExecutorId;
    this.outputWriterFlusher = new OutputWriterFlusher(flushPeriod);
  }

  public void initSetup(final ByteTransport byteTransport,
                        final PipeManagerWorker pipeManagerWorker) {
    this.byteTransport = byteTransport;
    this.pipeManagerWorker = pipeManagerWorker;
  }

  @Override
  protected void initChannel(final SocketChannel ch) {
    final ContextManager contextManager = new SimpleContextManagerImpl(
            byteTransport.getChannelGroup(),
            lambdaChannelMap,
            localExecutorId,
            ch,
            outputWriterFlusher);

    ch.pipeline()
      // inbound
      .addLast(new FrameDecoder(pipeManagerWorker, lambdaChannelMap))
      // outbound
      .addLast(controlFrameEncoder)
      .addLast(dataFrameEncoder)
      // inbound
      .addLast(contextManager);
  }
}
