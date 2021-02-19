package org.apache.nemo.runtime.executor.offloading;

import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.group.ChannelGroup;
import io.netty.channel.group.DefaultChannelGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.handler.codec.LengthFieldBasedFrameDecoder;
import io.netty.util.concurrent.GlobalEventExecutor;
import org.apache.nemo.conf.EvalConf;
import org.apache.nemo.offloading.client.*;
import org.apache.nemo.offloading.common.*;
import org.apache.nemo.runtime.executor.common.OutputWriterFlusher;
import org.apache.nemo.runtime.executor.common.datatransfer.ControlFrameEncoder;
import org.apache.nemo.runtime.executor.common.datatransfer.DataFrameEncoder;
import org.apache.reef.wake.remote.ports.TcpPortProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import java.util.List;
import java.util.Map;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

public final class DefaultOffloadingWorkerFactory implements OffloadingWorkerFactory {
  private static final Logger LOG = LoggerFactory.getLogger(DefaultOffloadingWorkerFactory.class.getName());

  private final ChannelGroup serverChannelGroup = new DefaultChannelGroup(GlobalEventExecutor.INSTANCE);
  private OffloadingEventHandler nemoEventHandler;
  private final ConcurrentMap<Channel, EventHandler<OffloadingEvent>> channelEventHandlerMap;

  private final NettyServerTransport workerControlTransport;
  private final AtomicBoolean initialized = new AtomicBoolean(false);

  private final AtomicInteger dataId = new AtomicInteger(0);
  private final AtomicInteger workerId = new AtomicInteger(0);

  private final NettyServerTransport workerDataTransport;
  /*
  private final AtomicInteger pendingRequest = new AtomicInteger(0);
  private final AtomicInteger extraRequest = new AtomicInteger(0);
  */

  private final OffloadingRequester requestor;

  private final ControlFrameEncoder controlFrameEncoder;
  private final DataFrameEncoder dataFrameEncoder;
  private final Map<String, Channel> dataTransportChannelMap;
  private final OutputWriterFlusher outputWriterFlusher;
  private final OffloadingFrameDecoder frameDecoder;

  @Inject
  private DefaultOffloadingWorkerFactory(final TcpPortProvider tcpPortProvider,
                                         final OffloadingRequesterFactory requesterFactory,
                                         final ControlFrameEncoder controlFrameEncoder,
                                         final EvalConf evalConf,
                                         final OffloadingFrameDecoder frameDecoder,
                                         final DataFrameEncoder dataFrameEncoder) {
    this.controlFrameEncoder = controlFrameEncoder;
    this.dataFrameEncoder = dataFrameEncoder;
    this.frameDecoder = frameDecoder;
    this.dataTransportChannelMap = new ConcurrentHashMap<>();
    this.channelEventHandlerMap = new ConcurrentHashMap<>();
    this.nemoEventHandler = new OffloadingEventHandler(channelEventHandlerMap);
    this.workerControlTransport = new NettyServerTransport(
      tcpPortProvider, new NettyChannelInitializer(
        new NettyServerSideChannelHandler(serverChannelGroup, nemoEventHandler)), false);
    this.workerDataTransport = new NettyServerTransport(
      tcpPortProvider, new WorkerDataTransportChannelInitializer(), false);
    this.outputWriterFlusher = new OutputWriterFlusher(evalConf.flushPeriod);

    LOG.info("Netty server lambda transport created end");
    initialized.set(true);

    this.requestor = requesterFactory.getInstance(
      nemoEventHandler, workerControlTransport.getPublicAddress(),
      workerControlTransport.getPort());
  }

  private void createChannelRequest() {
    //pendingRequest.getAndIncrement();
    requestor.createChannelRequest();
  }

  public void setVMAddressAndIds(final List<String> addr,
                                 final List<String> ids) {
    LOG.info("Set vm address and id in worker factory");
    if (requestor instanceof VMOffloadingRequester) {
      ((VMOffloadingRequester) requestor).setVmAddessesAndIds(addr, ids);
    }
  }

  @Override
  public OffloadingWorker createStreamingWorker(final ByteBuf workerInitBuffer,
                                                final OffloadingSerializer offloadingSerializer,
                                                final EventHandler eventHandler) {
    LOG.info("Create streaming worker request!");
    createChannelRequest();
    final Future<Pair<Channel, Pair<Channel, OffloadingEvent>>> channelFuture = new Future<Pair<Channel, Pair<Channel, OffloadingEvent>>>() {

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
      public Pair<Channel, Pair<Channel, OffloadingEvent>> get() throws InterruptedException, ExecutionException {
        final Pair<Channel, OffloadingEvent> pair;
        try {
          pair = nemoEventHandler.getHandshakeQueue().take();
        } catch (InterruptedException e) {
          e.printStackTrace();
          throw new RuntimeException(e);
        }

        pair.right().getByteBuf().release();

        LOG.info("Waiting worker init.. send buffer {}", workerInitBuffer.readableBytes());
        pair.left().writeAndFlush(new OffloadingEvent(OffloadingEvent.Type.WORKER_INIT, workerInitBuffer));

        final Pair<Channel, OffloadingEvent> workerDonePair = nemoEventHandler.getWorkerReadyQueue().take();
        final int len = workerDonePair.right().getByteBuf().readableBytes();
        final byte[] b = new byte[len];
        workerDonePair.right().getByteBuf().readBytes(b);
        final String dataAddr = new String(b);

        while (!dataTransportChannelMap.containsKey(dataAddr)) {
          LOG.warn("No data channel address for offlaod worker " + dataAddr);
          Thread.sleep(200);
        }

        // TODO: We configure data channel!!
        LOG.info("Waiting worker init done");
        return Pair.of(workerDonePair.left(), Pair.of(dataTransportChannelMap.get(dataAddr), workerDonePair.right()));
      }

      @Override
      public Pair<Channel, Pair<Channel, OffloadingEvent>> get(long timeout, TimeUnit unit) throws InterruptedException, ExecutionException, TimeoutException {
        return null;
      }
    };

    return new StreamingLambdaWorkerProxy(workerId.getAndIncrement(), channelFuture, this, channelEventHandlerMap,
      offloadingSerializer.getInputEncoder(), offloadingSerializer.getOutputDecoder(), eventHandler);
  }

  @Override
  public int getDataTransportPort() {
    return workerDataTransport.getPort();
  }

  @Override
  public void deleteOffloadingWorker(OffloadingWorker worker) {

    LOG.info("Delete prepareOffloading worker: {}", worker.getChannel().remoteAddress());

    final Channel channel = worker.getChannel();
    requestor.destroyChannel(channel);

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
}
