package org.apache.nemo.runtime.executor.offloading;

import io.netty.buffer.ByteBuf;
import io.netty.channel.*;
import io.netty.channel.group.ChannelGroup;
import io.netty.channel.group.DefaultChannelGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.handler.codec.LengthFieldBasedFrameDecoder;
import io.netty.util.concurrent.GlobalEventExecutor;
import org.apache.nemo.common.NettyServerTransport;
import org.apache.nemo.common.RuntimeIdManager;
import org.apache.nemo.conf.EvalConf;
import org.apache.nemo.conf.JobConf;
import org.apache.nemo.offloading.common.*;
import org.apache.nemo.runtime.common.comm.ControlMessage;
import org.apache.nemo.runtime.executor.NettyStateStore;
import org.apache.nemo.runtime.executor.common.OutputWriterFlusher;
import org.apache.nemo.runtime.executor.common.datatransfer.ControlFrameEncoder;
import org.apache.nemo.runtime.executor.common.datatransfer.DataFrameEncoder;
import org.apache.nemo.runtime.message.MessageContext;
import org.apache.nemo.runtime.message.MessageEnvironment;
import org.apache.nemo.runtime.message.MessageListener;
import org.apache.nemo.runtime.message.PersistentConnectionToMasterMap;
import org.apache.reef.tang.annotations.Parameter;
import org.apache.reef.wake.remote.ports.TcpPortProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import java.util.Map;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.apache.nemo.runtime.message.MessageEnvironment.ListenerType.LAMBDA_OFFLOADING_REQUEST_ID;

public final class DefaultOffloadingWorkerFactory implements OffloadingWorkerFactory {
  private static final Logger LOG = LoggerFactory.getLogger(DefaultOffloadingWorkerFactory.class.getName());

  private final ChannelGroup serverChannelGroup = new DefaultChannelGroup(GlobalEventExecutor.INSTANCE);

  private OffloadingDataTransportEventHandler nemoEventHandler;

  // private final NettyServerTransport workerControlTransport;

  private final AtomicBoolean initialized = new AtomicBoolean(false);

  private final AtomicInteger dataId = new AtomicInteger(0);
  private final AtomicInteger workerId = new AtomicInteger(0);

  private final NettyServerTransport workerDataTransport;
  /*
  private final AtomicInteger pendingRequest = new AtomicInteger(0);
  private final AtomicInteger extraRequest = new AtomicInteger(0);
  */
  public final ControlFrameEncoder controlFrameEncoder;
  private final DataFrameEncoder dataFrameEncoder;
  private final Map<String, Channel> dataTransportChannelMap;
  private final BlockingQueue<Pair<Integer, Channel>> dataTransportChannelQueue;
  private final OutputWriterFlusher outputWriterFlusher;
  private final OffloadingFrameDecoder frameDecoder;
  private final PersistentConnectionToMasterMap toMasterMap;
  private final MessageEnvironment messageEnvironment;
  private final String executorId;
  private final NettyStateStore nettyStateStore;
  private final String resourceType;

  @Inject
  private DefaultOffloadingWorkerFactory(final TcpPortProvider tcpPortProvider,
                                         final ControlFrameEncoder controlFrameEncoder,
                                         final EvalConf evalConf,
                                         final MessageEnvironment messageEnvironment,
                                         final PersistentConnectionToMasterMap toMasterMap,
                                         final OffloadingFrameDecoder frameDecoder,
                                         final DataFrameEncoder dataFrameEncoder,
                                         @Parameter(JobConf.ExecutorId.class) final String executorId,
                                         @Parameter(JobConf.ExecutorResourceType.class) final String resourceType,
                                         final NettyStateStore nettyStateStore,
                                         final OffloadingDataTransportEventHandler nemoEventHandler) {
    this.toMasterMap = toMasterMap;
    this.messageEnvironment = messageEnvironment;
    this.controlFrameEncoder = controlFrameEncoder;
    this.dataFrameEncoder = dataFrameEncoder;
    this.frameDecoder = frameDecoder;
    this.dataTransportChannelMap = new ConcurrentHashMap<>();
    this.dataTransportChannelQueue = new LinkedBlockingQueue<>();
    this.nemoEventHandler = nemoEventHandler;
    this.executorId = executorId;
    this.nettyStateStore = nettyStateStore;
    this.resourceType = resourceType;

    messageEnvironment
      .setupListener(LAMBDA_OFFLOADING_REQUEST_ID,
        new MessageReceiver());

    this.workerDataTransport = new NettyServerTransport(
      tcpPortProvider, new WorkerDataTransportChannelInitializer(),
      new NioEventLoopGroup(5),
      false);
    this.outputWriterFlusher = new OutputWriterFlusher(evalConf.flushPeriod);

    LOG.info("Netty server lambda transport created end");
    initialized.set(true);

  }

  public void start() {
    if (resourceType.equals("Compute")) {
      LOG.info("Send offloading request id");
      toMasterMap.getMessageSender(LAMBDA_OFFLOADING_REQUEST_ID)
        .send(ControlMessage.Message.newBuilder()
          .setId(RuntimeIdManager.generateMessageId())
          .setListenerId(LAMBDA_OFFLOADING_REQUEST_ID.ordinal())
          .setType(ControlMessage.MessageType.ExecutorPreparedForLambda)
          .setLambdaCreateMsg(ControlMessage.LambdaCreateMessage.newBuilder()
            .setDataChannelAddr(workerDataTransport.getPublicAddress())
            .setDataChannelPort(workerDataTransport.getPort())
            .setExecutorId(executorId)
            .setNettyStatePort(nettyStateStore.getPort())
            .setNumberOfLambda(1)
            .build())
          .build());
    }
  }

  private void createChannelRequest() {
    //pendingRequest.getAndIncrement();
    // Send message
    toMasterMap.getMessageSender(LAMBDA_OFFLOADING_REQUEST_ID)
      .send(ControlMessage.Message.newBuilder()
        .setId(RuntimeIdManager.generateMessageId())
        .setListenerId(LAMBDA_OFFLOADING_REQUEST_ID.ordinal())
        .setType(ControlMessage.MessageType.LambdaCreate)
        .setLambdaCreateMsg(ControlMessage.LambdaCreateMessage.newBuilder()
          .setDataChannelAddr(workerDataTransport.getPublicAddress())
          .setDataChannelPort(workerDataTransport.getPort())
          .setExecutorId(executorId)
          .setNettyStatePort(nettyStateStore.getPort())
          .setNumberOfLambda(1)
          .build())
        .build());
  }

  @Override
  public OffloadingWorker createStreamingWorker(final OffloadingSerializer offloadingSerializer,
                                                final EventHandler<Pair<OffloadingWorker, OffloadingExecutorControlEvent>> eventHandler) {
    LOG.info("Create streaming worker request!");
    createChannelRequest();
    final Future<Pair<Integer, Channel>> channelFuture = new Future<Pair<Integer, Channel>>() {

      @Override
      public boolean cancel(boolean mayInterruptIfRunning) {
        return false;
      }

      @Override
      public boolean isCancelled() {
        return false;
      }

      @Override
      public boolean isDone() {
        return false;
      }

      @Override
      public Pair<Integer, Channel> get() throws InterruptedException, ExecutionException {
        final Pair<Integer, Channel> event = dataTransportChannelQueue.take();
        LOG.info("Waiting worker init done... for workerRequestId {}, data channel: {}",
          event.left(), event.right());

        return event;
      }

      @Override
      public Pair<Integer, Channel> get(long timeout, TimeUnit unit) {
        return null;
      }
    };

    return new StreamingLambdaWorkerProxy(
      toMasterMap,
      channelFuture,
      nemoEventHandler.getChannelEventHandlerMap(),
      eventHandler);
  }

  @Override
  public int getDataTransportPort() {
    return workerDataTransport.getPort();
  }

  @Override
  public void deleteOffloadingWorker(OffloadingWorker worker) {

    LOG.info("Delete prepareOffloading worker: {}", worker.getChannel().remoteAddress());
    final Channel channel = worker.getChannel();

  }

  @ChannelHandler.Sharable
  final class WorkerDataTransportChannelInitializer extends ChannelInitializer<SocketChannel> {

    @Override
    protected void initChannel(SocketChannel ch) throws Exception {
      LOG.info("Data transport for prepareOffloading worker initialized {}", ch);
      outputWriterFlusher.registerChannel(ch);
      dataTransportChannelMap.put(ch.remoteAddress().toString(), ch);

      ch.pipeline()
        .addLast("frameDecoder", new LengthFieldBasedFrameDecoder(
        Integer.MAX_VALUE, 0, 4, 0, 4))
        // inbound
        .addLast(frameDecoder)
        // outbound
        .addLast(controlFrameEncoder)
        .addLast(dataFrameEncoder);
    }

    /**
     * Remove the inactive channel from channelGroup.
     * @param ctx the context object
     * @throws Exception
     */
    @Override
    public void channelInactive(final ChannelHandlerContext ctx) throws Exception {
      outputWriterFlusher.removeChannel(ctx.channel());
      LOG.info("Channel inactive {}", ctx.channel());
    }
  }

  public final class MessageReceiver implements MessageListener<ControlMessage.Message> {

    @Override
    public void onMessage(ControlMessage.Message message) {
      switch (message.getType()) {
        case LambdaControlChannel: {
          final ControlMessage.GetLambdaControlChannel m = message.getGetLambaControlChannelMsg();
          final String fullAddr = m.getFullAddr();
          final int requestId = (int) m.getRequestId();

          LOG.info("Receive GetLambdaControlChannel for {} / {}", fullAddr, requestId);

          while (!dataTransportChannelMap.containsKey(fullAddr)) {
            try {
              Thread.sleep(200);
            } catch (InterruptedException e) {
              e.printStackTrace();
            }
          }

          LOG.info("Receive end of GetLambdaControlChannel for {} / {}", fullAddr, requestId);

          dataTransportChannelQueue.add(Pair.of(requestId, dataTransportChannelMap.get(fullAddr)));

          break;
        }
        default:
          throw new RuntimeException("invalid message type " + message.getType());
      }
    }

    @Override
    public void onMessageWithContext(ControlMessage.Message message, MessageContext messageContext) {

    }
  }
}
