package org.apache.nemo.offloading.common;

import com.sun.management.OperatingSystemMXBean;
import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufInputStream;
import io.netty.buffer.ByteBufOutputStream;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.util.concurrent.DefaultThreadFactory;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import java.io.*;
import java.lang.management.ManagementFactory;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.*;

public final class OffloadingHandler {

  private static final Logger LOG = Logger.getLogger(OffloadingHandler.class.getName());

	//private static final String PATH = "/tmp/shaded.jar";
	private ClassLoader classLoader = null;
	private OffloadingTransform offloadingTransform = null;

	//private final String serializedUserCode = "rO0ABXNyABZRdWVyeTdTaWRlSW5wdXRIYW5kbGVyMlM6Ib0vAkQCAAB4cA==";
  /**
   * Netty event loop group for client worker.
   */
  private EventLoopGroup clientWorkerGroup;

  /**
   * Netty client bootstrap.
   */
  private Bootstrap clientBootstrap;

  private final ConcurrentMap<Channel, EventHandler<OffloadingMasterEvent>> map;

  // current states of lambda
  private LambdaStatus status;

  //private final Callable<ClassLoader> classLoaderCallable;

  private OffloadingEncoder outputEncoder;

  private LambdaOutputHandler outputCollector;

  private int dataProcessingCnt = 0;

  private final Map<String, LambdaEventHandler> lambdaEventHandlerMap;

  private ScheduledExecutorService workerHeartbeatExecutor;

  private final OperatingSystemMXBean operatingSystemMXBean;

  private transient CountDownLatch workerInitLatch;
  private transient int executorDataAddrPort;

  private final boolean isSf;

  private String nameServerAddr;
  private int nameServerPort;
  private String newExecutorId;

  private final long throttleRate;
  private final boolean testing;

  private final Map<String, TaskCaching> stageTaskMap = new HashMap<>();

	public OffloadingHandler(final Map<String, LambdaEventHandler> lambdaEventHandlerMap,
                           final boolean isSf,
                           final long throttleRate,
                           final boolean testing) {
    Logger.getRootLogger().setLevel(Level.INFO);
    this.lambdaEventHandlerMap = lambdaEventHandlerMap;
    this.isSf = isSf;
    this.throttleRate = throttleRate;
    this.testing = testing;

    this.operatingSystemMXBean =
      (OperatingSystemMXBean) ManagementFactory.getOperatingSystemMXBean();

		LOG.info("Handler is created!");
          this.clientWorkerGroup = new NioEventLoopGroup(1,
        new DefaultThreadFactory("hello" + "-ClientWorker"));
    this.clientBootstrap = new Bootstrap();
    this.map = new ConcurrentHashMap<>();
    this.clientBootstrap.group(clientWorkerGroup)
        .channel(NioSocketChannel.class)
        .handler(new NettyChannelInitializer(new NettyLambdaInboundHandler(map)))
        .option(ChannelOption.SO_REUSEADDR, true)
        .option(ChannelOption.SO_KEEPALIVE, true);
    this.status = LambdaStatus.INIT;
    //this.classLoaderCallable = classLoaderCallable;
	}

	public void setNameserverAddr(final String addr, final int port) {
	  this.nameServerAddr = addr;
	  this.nameServerPort = port;
  }

  public void setNewExecutorId(final String id) {
	  this.newExecutorId = id;
  }


  private Channel channelOpen(final String address, final int port) {
    // 1) connect to the VM worker

    while (true) {
      final ChannelFuture channelFuture;
      channelFuture = clientBootstrap.connect(new InetSocketAddress(address, port));
      channelFuture.awaitUninterruptibly();
      assert channelFuture.isDone();

      if (channelFuture.isCancelled()) {
        LOG.info("Channel future is cacelled...");
      } else if (!channelFuture.isSuccess()) {
        final StringBuilder sb = new StringBuilder("A connection failed at Source - .. retry connection");
        channelFuture.cause().printStackTrace();

        try {
          Thread.sleep(500);
        } catch (InterruptedException e) {
          e.printStackTrace();
        }
      } else {
        final Channel opendChannel = channelFuture.channel();
        return opendChannel;
      }
    }
  }

  private void writeResult(final Channel opendChannel,
                           final List<ChannelFuture> futures,
                           final Pair<Object, Integer> data) {
    final ByteBuf byteBuf = opendChannel.alloc().buffer();

    if (data.left() == NoResult.INSTANCE) {
      // bit 0 1
      byteBuf.writeByte(0);
      byteBuf.writeInt(data.right());
      byteBuf.writeInt(dataProcessingCnt);
    } else {
      byteBuf.writeByte(1);
      final ByteBufOutputStream bis = new ByteBufOutputStream(byteBuf);
      try {
        outputEncoder.encode(data.left(), bis);
        bis.writeInt(data.right());
        bis.writeInt(dataProcessingCnt);
        bis.close();
      } catch (IOException e) {
        e.printStackTrace();
        throw new RuntimeException(e);
      }
    }

    //System.out.println("Write result " + data.left().toString());

    opendChannel.writeAndFlush(
      new OffloadingMasterEvent(OffloadingMasterEvent.Type.RESULT, byteBuf));
  }

  private Channel handshake(final byte[] bytes,
                         final String address,
                         final int port,
                         Channel opendChannel,
                         final LinkedBlockingQueue<Pair<Object, Integer>> result) {

	  LambdaEventHandler handler = null;
	  Channel channel = opendChannel;

	  ChannelFuture channelFuture =
    channel.writeAndFlush(new OffloadingMasterEvent(OffloadingMasterEvent.Type.CLIENT_HANDSHAKE, bytes, bytes.length));

    while (!channelFuture.isSuccess()) {
      while (!channelFuture.isDone()) {
        LOG.info("Waiting client handshake done..");
        try {
          Thread.sleep(1000);
        } catch (InterruptedException e) {
          e.printStackTrace();
        }
      }

      if (!channelFuture.isSuccess()) {
        if (!channel.isOpen()) {
          channel = channelOpen(address, port);
          map.put(channel, new LambdaEventHandler(channel, result));
          handler = (LambdaEventHandler) map.get(channel);
        }
        LOG.info("Re-sending handshake..");
        channelFuture =
          channel.writeAndFlush(new OffloadingMasterEvent(OffloadingMasterEvent.Type.CLIENT_HANDSHAKE, bytes, bytes.length));
      } else {
        break;
      }
    }

    return channel;
  }

	public Object handleRequest(Map<String, Object> input) {
	  final long st = System.currentTimeMillis();
    this.workerHeartbeatExecutor = Executors.newSingleThreadScheduledExecutor();

		System.out.println("Input: " + input);
    final LinkedBlockingQueue<Pair<Object, Integer>> result = new LinkedBlockingQueue<>();

    offloadingTransform = null;

    // open channel
    Channel opendChannel = null;
    this.workerInitLatch = new CountDownLatch(1);

    for (final Map.Entry<Channel, EventHandler<OffloadingMasterEvent>> entry : map.entrySet()) {
      final Channel channel = entry.getKey();
      final String address = (String) input.get("address");
      final Integer port = (Integer) input.get("port");

      final String requestedAddr = "/" + address + ":" + port;

      System.out.println("Requested addr: " + requestedAddr +
        ", channelAddr: " +channel.remoteAddress().toString());

      if (channel.remoteAddress().toString().equals(requestedAddr)
        && channel.isOpen()) {
        opendChannel = channel;
        break;
      } else if (!channel.isOpen()) {
        channel.close();
        map.remove(channel);
      }
    }

    final String address = (String) input.get("address");
    final Integer port = (Integer) input.get("port");
    if (opendChannel == null) {
      opendChannel = channelOpen(address, port);
    }

    final int requestId = (Integer) input.get("requestId");

    map.put(opendChannel, new LambdaEventHandler(opendChannel, result));
    LambdaEventHandler handler = (LambdaEventHandler) map.get(opendChannel);

    System.out.println("Open channel: " + opendChannel);

    // load class loader

    if (classLoader == null) {
      System.out.println("Loading jar: " + opendChannel);
      try {
        //classLoader = classLoaderCallable.call();
        classLoader = Thread.currentThread().getContextClassLoader();
      } catch (Exception e) {
        e.printStackTrace();
        throw new RuntimeException(e);
      }
      status = LambdaStatus.READY;
      LOG.info("Create class loader: {}");
    }

    Thread.currentThread().setContextClassLoader(classLoader);

    // write handshake
    System.out.println("Data processing cnt: " + dataProcessingCnt
      + ", Write handshake: " + (System.currentTimeMillis() - st));

    byte[] bytes = ByteBuffer.allocate(4).putInt(requestId).array();

    opendChannel = handshake(bytes, address, port, opendChannel, result);
    handler = (LambdaEventHandler) map.get(opendChannel);

    // Waiting worker init done..
    LOG.info("Waiting worker init or end");

    if (handler == null) {
      LOG.info("handler is null for channel " + opendChannel);
      opendChannel = handshake(bytes, address, port, opendChannel, result);
      handler = (LambdaEventHandler) map.get(opendChannel);
    }

    while (workerInitLatch.getCount() > 0 && handler.endBlockingQueue.isEmpty()) {
      if (!opendChannel.isActive()) {
        opendChannel = handshake(bytes, address, port, opendChannel, result);
        handler = (LambdaEventHandler) map.get(opendChannel);
      }
      try {
        Thread.sleep(1000);
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
    }

    if (workerInitLatch.getCount() == 0) {
      final byte[] addrPortBytes = ByteBuffer.allocate(4).putInt(executorDataAddrPort).array();
      opendChannel.writeAndFlush(new OffloadingMasterEvent(OffloadingMasterEvent.Type.WORKER_INIT_DONE, addrPortBytes, addrPortBytes.length));
      LOG.info("Sending worker init done");
    }

    // cpu heartbeat
    final Channel ochannel = opendChannel;
    workerHeartbeatExecutor.scheduleAtFixedRate(() -> {
      final double cpuLoad = operatingSystemMXBean.getProcessCpuLoad();
      System.out.println("CPU Load: " + cpuLoad);
      final ByteBuf bb = ochannel.alloc().buffer();
      bb.writeDouble(cpuLoad);
      // ochannel.writeAndFlush(new OffloadingMasterEvent(OffloadingMasterEvent.Type.CPU_LOAD, bb));
    }, 2, 2, TimeUnit.SECONDS);


    // ready state
    //opendChannel.writeAndFlush(new OffloadingMasterEvent(OffloadingMasterEvent.Type.READY, new byte[0], 0));


		final List<ChannelFuture> futures = new LinkedList<>();

		// send result
    while (result.peek() != null || handler.endBlockingQueue.isEmpty()) {
      if (result.peek() != null) {
        final Pair<Object, Integer> data = result.poll();
        writeResult(opendChannel, futures, data);
      }

      try {
        Thread.sleep(10);
      } catch (InterruptedException e) {
        e.printStackTrace();
        throw new RuntimeException(e);
      }
    }

    final long sst = System.currentTimeMillis();

    /*
    futures.forEach(future -> {
      try {
        future.get();
      } catch (InterruptedException e) {
        e.printStackTrace();
        throw new RuntimeException(e);
      } catch (ExecutionException e) {
        e.printStackTrace();
        throw new RuntimeException(e);
      }
    });
    */

    try {
      // wait until end
      System.out.println("Wait end flag");
      final Integer endFlag = handler.endBlockingQueue.take();
      if (endFlag == 0) {
        System.out.println("end elapsed time: " + (System.currentTimeMillis() - sst));
        try {
          if (opendChannel.isOpen()) {
            opendChannel.writeAndFlush(new OffloadingMasterEvent(OffloadingMasterEvent.Type.END, new byte[0], 0)).get();
          } else {
            throw new RuntimeException("Channel is already closed..");
          }
        } catch (InterruptedException e) {
          e.printStackTrace();
          throw new RuntimeException(e);
        } catch (ExecutionException e) {
          e.printStackTrace();
          throw new RuntimeException(e);
        }
      } else {
        // warm up end... just finish
      }

      System.out.println("END of invocation: " + (System.currentTimeMillis() - sst));
    } catch (InterruptedException e) {
      e.printStackTrace();
      throw new RuntimeException(e);
    }

    workerHeartbeatExecutor.shutdown();

    LOG.info("Finishing channels");
    map.entrySet().forEach(entry -> {
      entry.getKey().close().awaitUninterruptibly();
    });

    map.clear();

    return null;
	}

  public final class LambdaEventHandler implements EventHandler<OffloadingMasterEvent> {

    private final BlockingQueue<Integer> endBlockingQueue = new LinkedBlockingQueue<>();
    private final Channel opendChannel;
    private final BlockingQueue<Pair<Object, Integer>> result;
    private OffloadingDecoder decoder;

    private long workerFinishTime;

    public LambdaEventHandler(final Channel opendChannel,
                              final BlockingQueue<Pair<Object, Integer>> result) {
      this.opendChannel = opendChannel;
      this.result = result;
    }

    @Override
    public synchronized void onNext(final OffloadingMasterEvent nemoEvent) {
      switch (nemoEvent.getType()) {
        case VM_SCALING_INFO: {
          // It receives global information such as name server address ...
          final ByteBuf byteBuf = nemoEvent.getByteBuf();
          final ByteBufInputStream bis = new ByteBufInputStream(byteBuf);
          final DataInputStream dataInputStream = new DataInputStream(bis);
          try {
            nameServerAddr = dataInputStream.readUTF();
            nameServerPort = dataInputStream.readInt();
            newExecutorId = dataInputStream.readUTF();

            System.out.println(
              "VM Scaling info..  nameServerAddr: " + nameServerAddr
                + ", nameSeverPort: " + nameServerPort
              + ", executorID: " + newExecutorId);

          } catch (IOException e) {
            e.printStackTrace();
            throw new RuntimeException(e);
          }
          break;
        }
        case WORKER_INIT: {
          System.out.println("Worker init... bytes: " + nemoEvent.getByteBuf().readableBytes());
          final long st = System.currentTimeMillis();
          // load transforms
          final ByteBuf byteBuf = nemoEvent.getByteBuf();
          ByteBufInputStream bis = new ByteBufInputStream(byteBuf);
          //System.out.println("Serialized transforms size: " + bytes.length);
          //ByteArrayInputStream bis = new ByteArrayInputStream(bytes);
          try {
            Thread.currentThread().setContextClassLoader(classLoader);
            ObjectInputStream ois = new ExternalJarObjectInputStream(classLoader, bis);
            System.out.println("Before OffloadingTransform: ");
            offloadingTransform = (OffloadingTransform) ois.readObject();
            System.out.println("After OffloadingTransform: ");
            decoder = (OffloadingDecoder) ois.readObject();
            outputEncoder = (OffloadingEncoder) ois.readObject();

            System.out.println("OffloadingTransform: " + offloadingTransform);

            ois.close();
            bis.close();
            byteBuf.release();
          } catch (IOException e) {
            e.printStackTrace();
            throw new RuntimeException(e);
          } catch (ClassNotFoundException e) {
            e.printStackTrace();
            throw new RuntimeException(e);
          }

          outputCollector = new LambdaOutputHandler(result);

          LOG.info("Before offloading prepare, stageTaskMap size: " + stageTaskMap.size());

          // TODO: OffloadingTransform that receives data from parent tasks should register its id
          // to lambdaEventHandlerMap
          offloadingTransform.prepare(
            new LambdaRuntimeContext(lambdaEventHandlerMap, this, isSf,
              nameServerAddr, nameServerPort, newExecutorId, opendChannel, throttleRate,
              testing, stageTaskMap), outputCollector);

          LOG.info("End of offloading prepare");

          workerFinishTime = System.currentTimeMillis();
          executorDataAddrPort = offloadingTransform.getDataChannelPort();
          System.out.println("End of worker init: " + (System.currentTimeMillis() - st) + ", data channel: " + executorDataAddrPort);
          workerInitLatch.countDown();

          break;
        }
        case TASK_SEND: {
          final long st = System.currentTimeMillis();
          Thread.currentThread().setContextClassLoader(classLoader);
          //System.out.println("Worker init -> data time: " + (st - workerFinishTime) +
          // " databytes: " + nemoEvent.getByteBuf().readableBytes());

          System.out.println("Decodable size: " + nemoEvent.getByteBuf().readableBytes());

          final ByteBufInputStream bis = new ByteBufInputStream(nemoEvent.getByteBuf());
          try {
            final String taskId = bis.readUTF();
            final Object data = decoder.decode(bis);
            offloadingTransform.onData(data, null);
            outputCollector.hasDataReceived = false;
            //System.out.println("Data processing done: " + (System.currentTimeMillis() - st));
            dataProcessingCnt += 1;

            nemoEvent.getByteBuf().release();

            final ByteBufOutputStream bos = new ByteBufOutputStream(
              offloadingTransform.getDataChannel().alloc().ioBuffer());

            bos.writeUTF(taskId);
            bos.close();
            LOG.info("Send task ready for " + taskId + ", readable byte " + bos.buffer().readableBytes());

            offloadingTransform.getDataChannel()
              .writeAndFlush(new OffloadingExecutorControlEvent(
                OffloadingExecutorControlEvent.Type.TASK_READY, bos.buffer()));

            // opendChannel.writeAndFlush(new OffloadingMasterEvent(
            //  OffloadingMasterEvent.Type.TASK_READY, bos.buffer()));

          } catch (IOException e) {
            if (e.getMessage().contains("EOF")) {
              System.out.println("eof!");
            } else {
              e.printStackTrace();
              throw new RuntimeException(e);
            }
          }
          break;
        }
        case END:
          // send result
          System.out.println("Offloading end");
          if (offloadingTransform != null) {
            offloadingTransform.close();
          }
          nemoEvent.getByteBuf().release();
          endBlockingQueue.add(0);
          // end of event
          // update handler
          break;
        case WARMUP_END:
          System.out.println("Warmup end");
          nemoEvent.getByteBuf().release();
          endBlockingQueue.add(1);
          break;
        default:
          throw new RuntimeException("Invalid type " + nemoEvent.getType());
      }
    }
  }

	final class LambdaOutputHandler  implements OffloadingOutputCollector {

	  private final BlockingQueue<Pair<Object, Integer>> result;
	  private int dataId;
	  public boolean hasDataReceived = false;

	  public LambdaOutputHandler(final BlockingQueue<Pair<Object, Integer>> result) {
	    this.result = result;
    }

    void setDataId(final int id) {
	    dataId = id;
    }

    @Override
    public void emit(Object output) {
      //System.out.println("Emit output of data " + output.toString());
      result.add(Pair.of(output, dataId));
      hasDataReceived = true;
    }
  }

  static final class NoResult {
    public static final NoResult INSTANCE = new NoResult();
    private NoResult() {
    }
  }
}
