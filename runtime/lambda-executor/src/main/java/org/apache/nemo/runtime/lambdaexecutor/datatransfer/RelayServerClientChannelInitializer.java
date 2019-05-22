package org.apache.nemo.runtime.lambdaexecutor.datatransfer;

import io.netty.channel.ChannelInitializer;
import io.netty.channel.group.ChannelGroup;
import io.netty.channel.socket.SocketChannel;
import org.apache.nemo.runtime.executor.common.datatransfer.*;
import org.apache.nemo.runtime.executor.common.relayserverclient.RelayClientDecoder;
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


  public RelayServerClientChannelInitializer(final ChannelGroup channelGroup,
                                             final ControlFrameEncoder controlFrameEncoder,
                                             final DataFrameEncoder dataFrameEncoder,
                                             final ConcurrentMap<SocketChannel, Boolean> channels,
                                             final String localExecutorId,
                                             final AckScheduledService ackScheduledService,
                                             final Map<TransferKey, Integer> taskTransferIndexMap) {
    this.channelGroup = channelGroup;
    this.controlFrameEncoder = controlFrameEncoder;
    this.dataFrameEncoder = dataFrameEncoder;
    this.localExecutorId = localExecutorId;
    this.channels = channels;
    this.ackScheduledService = ackScheduledService;
    this.taskTransferIndexMap = taskTransferIndexMap;
    this.channelExecutorService = Executors.newCachedThreadPool();
  }

  public void setRelayServerClient(final RelayServerClient client) {
    relayServerClient = client;
  }

  @Override
  protected void initChannel(SocketChannel ch) throws Exception {
    LOG.info("Registering channel {}", ch.remoteAddress());

    final ContextManager contextManager = new LambdaContextManager(
      channelExecutorService,
      channelGroup, localExecutorId, ch, ackScheduledService, taskTransferIndexMap, true,
      relayServerClient);

    ch.pipeline()
      // outbound
      .addLast(new RelayControlFrameEncoder())
      .addLast(new RelayDataFrameEncoder())
      .addLast(new RelayControlMessageEncoder())
      .addLast(new RelayDebuggingEncoder())

      // inbound
      .addLast(new FrameDecoder(contextManager))
      .addLast(contextManager);
  }
}
