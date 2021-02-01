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
package org.apache.nemo.runtime.executor.bytetransfer;

import org.apache.nemo.common.TaskLocationMap;
import org.apache.nemo.conf.EvalConf;
import org.apache.nemo.conf.JobConf;
import org.apache.nemo.runtime.common.message.PersistentConnectionToMasterMap;
import org.apache.nemo.runtime.executor.common.OutputWriterFlusher;
import org.apache.nemo.runtime.executor.common.datatransfer.*;
import org.apache.nemo.runtime.executor.data.BlockManagerWorker;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.socket.SocketChannel;
import org.apache.nemo.runtime.executor.common.datatransfer.PipeManagerWorker;
import org.apache.nemo.runtime.executor.PipeIndexMapWorker;
import org.apache.nemo.runtime.executor.relayserver.RelayServer;
import org.apache.reef.tang.annotations.Parameter;

import javax.inject.Inject;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

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
public final class ByteTransportChannelInitializer extends ChannelInitializer<SocketChannel> {

  private PipeManagerWorker pipeManagerWorker;
  private BlockManagerWorker blockManagerWorker;
  private ByteTransfer byteTransfer;
  private ByteTransport byteTransport;
  private AckScheduledService ackScheduledService;
  private final ControlFrameEncoder controlFrameEncoder;
  private final DataFrameEncoder dataFrameEncoder;
  private final String localExecutorId;
  private final PipeIndexMapWorker taskTransferIndexMap;


  private final ConcurrentMap<Integer, ByteInputContext> inputContexts = new ConcurrentHashMap<>();
  private final ConcurrentMap<Integer, ByteOutputContext> outputContexts = new ConcurrentHashMap<>();
  //private final ConcurrentMap<Integer, ByteInputContext> inputContextsInitiatedByRemote = new ConcurrentHashMap<>();
  //private final ConcurrentMap<Integer, ByteOutputContext> outputContextsInitiatedByRemote = new ConcurrentHashMap<>();
  private final ExecutorService channelServiceExecutor;
  private final PersistentConnectionToMasterMap toMaster;

  private final OutputWriterFlusher outputWriterFlusher;
  private final RelayServer relayServer;
  private final TaskLocationMap taskLocationMap;

  /**
   * Creates a netty channel initializer.
   *
   * @param controlFrameEncoder encodes control frames
   * @param dataFrameEncoder    encodes data frames
   * @param localExecutorId     the id of this executor
   */
  @Inject
  private ByteTransportChannelInitializer(
                                          final ControlFrameEncoder controlFrameEncoder,
                                          final DataFrameEncoder dataFrameEncoder,
                                          final PipeIndexMapWorker taskTransferIndexMap,
                                          @Parameter(JobConf.ExecutorId.class) final String localExecutorId,
                                          final PersistentConnectionToMasterMap toMaster,
                                          final EvalConf evalConf,
                                          final RelayServer relayServer,
                                          final TaskLocationMap taskLocationMap) {

    this.controlFrameEncoder = controlFrameEncoder;
    this.dataFrameEncoder = dataFrameEncoder;
    this.localExecutorId = localExecutorId;
    this.taskTransferIndexMap = taskTransferIndexMap;
    this.channelServiceExecutor = Executors.newCachedThreadPool();
    this.toMaster = toMaster;
    this.outputWriterFlusher = new OutputWriterFlusher(evalConf.flushPeriod);
    this.relayServer = relayServer;
    this.taskLocationMap = taskLocationMap;
  }

  public void initSetup(final PipeManagerWorker pipeManagerWorker,
                        final BlockManagerWorker blockManagerWorker,
                        final ByteTransfer byteTransfer,
                        final ByteTransport byteTransport,
                        final AckScheduledService ackScheduledService) {
    this.pipeManagerWorker = pipeManagerWorker;
    this.blockManagerWorker = blockManagerWorker;
    this.byteTransfer = byteTransfer;
    this.byteTransport = byteTransport;
    this.ackScheduledService = ackScheduledService;
  }

  @Override
  protected void initChannel(final SocketChannel ch) {
    final ContextManager contextManager = new DefaultContextManagerImpl(
            channelServiceExecutor,
            pipeManagerWorker,
            blockManagerWorker,
            Optional.of(byteTransfer),
            byteTransport.getChannelGroup(),
            localExecutorId,
            ch,
            //vmScalingClientTransport.get(),
             ackScheduledService,
            inputContexts,
            outputContexts,
            toMaster,
            outputWriterFlusher,
            relayServer,
            taskLocationMap);

    ch.pipeline()
      // inbound
      .addLast(new FrameDecoder(pipeManagerWorker))
      // outbound
      .addLast(controlFrameEncoder)
      .addLast(dataFrameEncoder)
      // inbound
      .addLast(contextManager);
  }
}
