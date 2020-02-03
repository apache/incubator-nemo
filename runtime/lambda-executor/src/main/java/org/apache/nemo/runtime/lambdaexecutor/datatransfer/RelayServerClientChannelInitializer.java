package org.apache.nemo.runtime.lambdaexecutor.datatransfer;

import io.netty.channel.ChannelInitializer;
import io.netty.channel.group.ChannelGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.handler.codec.LengthFieldPrepender;
import org.apache.nemo.common.TaskLoc;
import org.apache.nemo.common.TaskLocationMap;
import org.apache.nemo.common.TransferKey;
import org.apache.nemo.runtime.executor.common.OutputWriterFlusher;
import org.apache.nemo.runtime.executor.common.datatransfer.*;
import org.apache.nemo.runtime.executor.common.relayserverclient.RelayControlFrameEncoder;
import org.apache.nemo.runtime.executor.common.relayserverclient.RelayControlMessageEncoder;
import org.apache.nemo.runtime.executor.common.relayserverclient.RelayDataFrameEncoder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public final class RelayServerClientChannelInitializer extends ChannelInitializer<SocketChannel> {
  private static final Logger LOG = LoggerFactory.getLogger(RelayServerClientChannelInitializer.class);

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
  final ConcurrentMap<Integer, ByteOutputContext> outputContextMap;;
  private ByteTransfer byteTransfer;
  private final OutputWriterFlusher outputWriterFlusher;
  private final TaskLocationMap taskLocationMap;
  private final Map<String, String> taskExecutorIdMap;
  private final RendevousServerClient rendevousServerClient;

  public RelayServerClientChannelInitializer(final ChannelGroup channelGroup,
                                             final ControlFrameEncoder controlFrameEncoder,
                                             final DataFrameEncoder dataFrameEncoder,
                                             final ConcurrentMap<SocketChannel, Boolean> channels,
                                             final String localExecutorId,
                                             final AckScheduledService ackScheduledService,
                                             final Map<TransferKey, Integer> taskTransferIndexMap,
                                             final ConcurrentMap<Integer, ByteInputContext> inputContextMap,
                                             final ConcurrentMap<Integer, ByteOutputContext> outputContextMap,
                                             final OutputWriterFlusher outputWriterFlusher,
                                             final TaskLocationMap taskLocationMap,
                                             final Map<String, String> taskExecutorIdMap,
                                             final RendevousServerClient rendevousServerClient) {
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
    this.taskLocationMap = taskLocationMap;
    this.taskExecutorIdMap = taskExecutorIdMap;
    this.rendevousServerClient = rendevousServerClient;
  }

  public void setRelayServerClient(final RelayServerClient client) {
    relayServerClient = client;
  }

  public void setByteTransfer(final ByteTransfer bt) {
    byteTransfer = bt;
  }

  @Override
  protected void initChannel(SocketChannel ch) throws Exception {
    //LOG.info("Registering channel {}", ch);

    final ContextManager contextManager = new LambdaContextManager(
      channelExecutorService,
      inputContextMap,
      outputContextMap,
      channelGroup, localExecutorId, ch, ackScheduledService, taskTransferIndexMap, true,
      relayServerClient, byteTransfer, outputWriterFlusher, TaskLoc.SF, taskLocationMap, taskExecutorIdMap,
      rendevousServerClient);

    ch.pipeline()
      // outbound
      .addLast("frameEncoder", new LengthFieldPrepender(4))

      .addLast(new RelayControlFrameEncoder())
      .addLast(new RelayDataFrameEncoder())
      .addLast(new RelayControlMessageEncoder())
      .addLast(new RelayDebuggingEncoder())

      // inbound
      .addLast(new FrameDecoder(contextManager))
      .addLast(contextManager);
  }
}
