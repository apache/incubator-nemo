package org.apache.nemo.offloading.workers.vm;

import com.google.protobuf.ByteString;
import io.netty.bootstrap.Bootstrap;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufInputStream;
import io.netty.channel.*;
import io.netty.channel.group.ChannelGroup;
import io.netty.channel.group.DefaultChannelGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.util.concurrent.DefaultThreadFactory;
import io.netty.util.concurrent.GlobalEventExecutor;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.io.UnboundedSource;
import org.apache.commons.lang3.SerializationUtils;
import org.apache.nemo.common.Pair;
import org.apache.nemo.common.TaskLoc;
import org.apache.nemo.common.TransferKey;
import org.apache.nemo.common.VMWorkerConf;

import org.apache.nemo.common.coder.FSTSingleton;
import org.apache.nemo.compiler.frontend.beam.transform.GBKFinalState;
import org.apache.nemo.offloading.common.*;
import org.apache.nemo.runtime.executor.common.Serializer;
import org.apache.nemo.runtime.lambdaexecutor.ReadyTask;
import org.apache.nemo.runtime.lambdaexecutor.general.OffloadingExecutorInputDecoder;
import org.apache.nemo.runtime.lambdaexecutor.general.OffloadingTask;
import org.apache.nemo.runtime.lambdaexecutor.middle.MiddleOffloadingOutputEncoder;
import org.json.JSONObject;
import org.nustaq.serialization.FSTConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.DataInputStream;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

public class VMWorker {
  private static final Logger LOG = LoggerFactory.getLogger(VMWorker.class.getName());
  private static final int SERVER_BOSS_NUM_THREADS = 3;
  private static final int SERVER_WORKER_NUM_THREADS = 15;
  private static final String CLASS_NAME = VMWorker.class.getName();

  private static final String ADDRESS = "0.0.0.0";

  private final ChannelGroup serverChannelGroup = new DefaultChannelGroup(GlobalEventExecutor.INSTANCE);
  private EventLoopGroup serverBossGroup;
  private EventLoopGroup serverWorkerGroup;
  private Channel acceptor;

  private final ExecutorService executorService = Executors.newCachedThreadPool();
  private final ExecutorService singleThread = Executors.newSingleThreadExecutor();

  private final OffloadingDecoder decoder;
  private final OffloadingEncoder outputEncoder;

  private VmOffloadingExecutor executor;

  private final AtomicBoolean executorInitalized = new AtomicBoolean(false);

  private VMWorker() {

    serverBossGroup = new NioEventLoopGroup(SERVER_BOSS_NUM_THREADS,
      new DefaultThreadFactory(CLASS_NAME + "SourceServerBoss"));
    serverWorkerGroup = new NioEventLoopGroup(SERVER_WORKER_NUM_THREADS,
      new DefaultThreadFactory(CLASS_NAME + "SourceServerWorker"));

    this.decoder = new OffloadingExecutorInputDecoder();
    this.outputEncoder = new MiddleOffloadingOutputEncoder();

    final BlockingQueue<Pair<OffloadingEvent, Channel>> requestQueue = new LinkedBlockingQueue<>();
    singleThread.execute(() -> {
      while (true) {
        try {
          final Pair<OffloadingEvent, Channel> pair = requestQueue.take();
          final OffloadingEvent event = pair.left();
          final Channel c = pair.right();
          final VMOutputCollector oc = new VMOutputCollector(c);

            switch (event.getType()) {
              case CONNECT: {
                // Initiated by master

                System.out.println("Worker init... bytes: " + event.getByteBuf().readableBytes());
                final long st = System.currentTimeMillis();
                // load transforms
                final ByteBuf byteBuf = event.getByteBuf();
                final VMWorkerConf vmWorkerConf = VMWorkerConf.decode(byteBuf);

                try {
                  final Map<String, InetSocketAddress> executorAddrMap1 =
                    vmWorkerConf.executorAddrMap.entrySet().stream()
                    .collect(Collectors.toMap(Map.Entry::getKey, e -> {
                      final Pair<String, Integer> p = e.getValue();
                      return new InetSocketAddress(p.left(), p.right());
                    }));

                  final Map<String, Serializer> serializerMap1 =
                    vmWorkerConf.serializerMap.entrySet().stream()
                    .collect(Collectors.toMap(Map.Entry::getKey, e -> {
                      final byte[] b = e.getValue();
                      return SerializationUtils.deserialize(b);
                    }));

                  final ByteBufInputStream bis = new ByteBufInputStream(byteBuf);
                  final String executorId = bis.readUTF();

                  LOG.info("My executor id {}", executorId);

                  executor =
                    new VmOffloadingExecutor(vmWorkerConf.executorThreadNum,
                      executorAddrMap1,
                      serializerMap1,
                      vmWorkerConf.taskExecutorIdMap,
                      vmWorkerConf.taskTransferIndexMap,
                      vmWorkerConf.rendevousServerAddr,
                      vmWorkerConf.rendevousServerPort,
                      vmWorkerConf.nameServerAddr,
                      vmWorkerConf.nameServerPort,
                      executorId,
                      TaskLoc.VM_SCALING);

                  byteBuf.release();

                  executor.prepare(null, new VMOutputCollector(c));

                  System.out.println("End of worker init: " + (System.currentTimeMillis() - st));

                  c.writeAndFlush(new OffloadingEvent(OffloadingEvent.Type.CONNECT_DONE, new byte[0], 0));
                } catch (IOException e) {
                  e.printStackTrace();
                  throw new RuntimeException(e);
                }
                break;
              }
              case EXECUTOR_INIT_INFO: {

                final ByteBuf byteBuf = event.getByteBuf();
                final ByteBufInputStream bis = new ByteBufInputStream(byteBuf);
                final FSTConfiguration conf = FSTSingleton.getInstance();

                try {
                  final Map<String, TaskLoc> taskLocMap =
                    (Map<String, TaskLoc>) conf.decodeFromStream(bis);
                  final Map<String, String> taskExecutorIdMap =
                    (Map<String, String>) conf.decodeFromStream(bis);


                  LOG.info("Receiveing executor init info: " +
                      "taskExecutorIdMap: {}, taskLocMap: {}",
                    taskExecutorIdMap, taskLocMap);

                  executor.setExecutorInitInfo(taskLocMap, taskExecutorIdMap);

                  executorInitalized.set(true);
                } catch (final Exception e) {
                  e.printStackTrace();
                  throw new RuntimeException(e);
                }
                byteBuf.release();
                break;
              }
              case OFFLOADING_TASK: {
                final ByteBuf byteBuf = event.getByteBuf();
                final ByteBufInputStream bis = new ByteBufInputStream(byteBuf);

                LOG.info("Readable byte of offloading task {}", byteBuf.readableBytes());

                final int taskStartInt = byteBuf.readInt();

                executorService.execute(() -> {
                  try {
                    final OffloadingTask offloadingTask = OffloadingTask.decode(bis);

                    LOG.info("Receive offloading task {} / {}", offloadingTask.taskId, offloadingTask.executorId);

                    while (!executorInitalized.get()) {
                      Thread.sleep(500);
                    }

                    executor.onData(offloadingTask, oc);
                    LOG.info("End of handling offloading task {} / {}", offloadingTask.taskId, offloadingTask.executorId);

                    byteBuf.release();
                  } catch (final Exception e) {
                    e.printStackTrace();
                    throw new RuntimeException(e);
                  }
                });

                break;
              }
              case MIDDLE_TASK: {
                final ByteBuf byteBuf = event.getByteBuf();
                final ByteBufInputStream bis = new ByteBufInputStream(byteBuf);
                final DataInputStream dis = new DataInputStream(bis);

                executorService.execute(() -> {
                  try {
                    final boolean mvToVmScaling = dis.readBoolean();
                    final String taskId = dis.readUTF();
                    final int mapSize = dis.readInt();
                    final Map<String, GBKFinalState> stateMap = new HashMap<>();
                    final Map<String, Coder<GBKFinalState>> stateCoderMap = new HashMap<>();
                    for (int i = 0; i < mapSize; i++) {
                      final String key = dis.readUTF();
                      final Coder<GBKFinalState> coder = SerializationUtils.deserialize(dis);
                      final GBKFinalState state = coder.decode(dis);
                      stateMap.put(key, state);
                      stateCoderMap.put(key, coder);
                    }

                    final ReadyTask readyTask =
                      new ReadyTask(taskId,
                        new HashMap<>(),
                        null,
                        null,
                        0,
                        null,
                        stateMap,
                        stateCoderMap,
                        new HashMap<>());

                    while (!executorInitalized.get()) {
                      Thread.sleep(500);
                    }

                    executor.onData(readyTask, oc);

                  } catch (final Exception e) {
                    e.printStackTrace();
                    throw new RuntimeException(e);
                  }

                  byteBuf.release();
                });
                break;
              }
              case SOURCE_TASK: {
                final ByteBuf byteBuf = event.getByteBuf();
                final ByteBufInputStream bis = new ByteBufInputStream(byteBuf);
                final DataInputStream dis = new DataInputStream(bis);

                executorService.execute(() -> {
                  try {
                    final long prevWatermark = dis.readLong();
                    final FSTConfiguration conf = FSTSingleton.getInstance();
                    final UnboundedSource s = (UnboundedSource) conf.decodeFromStream(dis);

                    final boolean mvToScaling = dis.readBoolean();
                    final String taskId = dis.readUTF();
                    final int id = dis.readInt();

                    final Coder<UnboundedSource.CheckpointMark> checkpointMarkCoder = SerializationUtils.deserialize(dis);
                    final UnboundedSource.CheckpointMark checkpointMark = checkpointMarkCoder.decode(dis);

                    final long e = System.currentTimeMillis();

                    final int mapSize = dis.readInt();
                    final Map<String, GBKFinalState> stateMap = new HashMap<>();
                    final Map<String, Coder<GBKFinalState>> stateCoderMap = new HashMap<>();
                    for (int i = 0; i < mapSize; i++) {
                      final String key = dis.readUTF();
                      final Coder<GBKFinalState> coder = SerializationUtils.deserialize(dis);
                      final GBKFinalState state = coder.decode(dis);
                      stateMap.put(key, state);
                      stateCoderMap.put(key, coder);
                    }

                    LOG.info("Map decoding time of {}: {}", taskId, System.currentTimeMillis() - e);

                    final ReadyTask readyTask = new ReadyTask(
                      taskId,
                      new HashMap<>(),
                      checkpointMark,
                      checkpointMarkCoder,
                      prevWatermark,
                      s,
                      stateMap,
                      stateCoderMap,
                      new HashMap<>());

                    while (!executorInitalized.get()) {
                      Thread.sleep(500);
                    }

                    executor.onData(readyTask, oc);

                  } catch (final Exception e) {
                    e.printStackTrace();
                    throw new RuntimeException(e);
                  }

                  byteBuf.release();
                });
                break;
              }
              default:
                throw new RuntimeException("unsupported type: " + event.getType());
            }

        } catch (InterruptedException e) {
          e.printStackTrace();
        }
      }
    });


    final ServerBootstrap serverBootstrap = new ServerBootstrap();
    serverBootstrap.group(serverBossGroup, serverWorkerGroup)
      .channel(NioServerSocketChannel.class)
      .childHandler(new NettyChannelInitializer(
        new NettyServerSideChannelHandler(serverChannelGroup, requestQueue)))
      .option(ChannelOption.SO_BACKLOG, 128)
      .option(ChannelOption.SO_REUSEADDR, true)
      .childOption(ChannelOption.SO_KEEPALIVE, true);
    try {
      acceptor = serverBootstrap.bind(
        new InetSocketAddress(ADDRESS, Constants.VM_WORKER_PORT)).sync().channel();
    } catch (InterruptedException e) {
      e.printStackTrace();
      throw new RuntimeException(e);
    }
  }


  public static void main(final String[] args) throws InterruptedException {
    final CountDownLatch countDownLatch = new CountDownLatch(1);
    LOG.info("Start worker");
    final VMWorker worker = new VMWorker();
    LOG.info("Wait for request");
    countDownLatch.await();
  }


  @ChannelHandler.Sharable
  final class NettyServerSideChannelHandler extends ChannelInboundHandlerAdapter {
    private final Logger LOG = LoggerFactory.getLogger(NettyServerSideChannelHandler.class.getName());
    private final ChannelGroup channelGroup;
    private final BlockingQueue<Pair<OffloadingEvent, Channel>> requestQueue;

    NettyServerSideChannelHandler(final ChannelGroup channelGroup,
                                  final BlockingQueue<Pair<OffloadingEvent, Channel>> requestQueue) {
      this.channelGroup = channelGroup;
      this.requestQueue = requestQueue;
    }

    /**
     * Add the active channel to channelGroup.
     * @param ctx the context object
     * @throws Exception
     */
    @Override
    public void channelActive(final ChannelHandlerContext ctx) throws Exception {
      LOG.info("Channel activate: {}", ctx.channel());
      channelGroup.add(ctx.channel());
    }

    @Override
    public void channelRead(final ChannelHandlerContext ctx, final Object msg) throws Exception {
      LOG.info("Channel read from {}, {}", ctx.channel(), msg);
      requestQueue.add(Pair.of((OffloadingEvent)msg, ctx.channel()));
    }

    /**
     * Remove the inactive channel from channelGroup.
     * @param ctx the context object
     * @throws Exception
     */
    @Override
    public void channelInactive(final ChannelHandlerContext ctx) throws Exception {
      channelGroup.remove(ctx);
      ctx.close();
    }

    @Override
    public void exceptionCaught(final ChannelHandlerContext ctx, final Throwable cause) throws Exception {
      cause.printStackTrace();
      channelGroup.remove(ctx);
      ctx.close();
    }
  }
}
