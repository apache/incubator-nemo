package org.apache.nemo.runtime.lambdaexecutor;

import com.amazonaws.services.lambda.runtime.Context;
import com.sun.management.OperatingSystemMXBean;
import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufInputStream;
import io.netty.buffer.ByteBufOutputStream;
import io.netty.channel.*;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.util.concurrent.DefaultThreadFactory;
import org.apache.commons.lang3.SerializationUtils;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.nemo.offloading.common.*;
import org.apache.nemo.runtime.executor.common.CpuInfoExtractor;
import org.apache.nemo.runtime.executor.common.controlmessages.TaskControlMessage;
import org.apache.nemo.runtime.lambdaexecutor.general.OffloadingExecutor;
import org.apache.nemo.runtime.lambdaexecutor.general.OffloadingExecutorInputDecoder;
import org.apache.nemo.runtime.lambdaexecutor.middle.MiddleOffloadingOutputEncoder;

import java.io.*;
import java.lang.management.ManagementFactory;
import java.lang.management.RuntimeMXBean;
import java.net.*;
import java.nio.ByteBuffer;
import java.util.*;
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
  private Channel controlChannel;
  // private Channel dataChannel;
  private LambdaEventHandler handler;
  private int requestId;

  private final BlockingQueue<Integer> endBlockingQueue = new LinkedBlockingQueue<>();

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
          map.put(channel, new LambdaEventHandler(result));
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

  private void initialization(Map<String, Object> input) {
	  final long st = System.currentTimeMillis();

    this.workerInitLatch = new CountDownLatch(1);

    this.workerHeartbeatExecutor = Executors.newSingleThreadScheduledExecutor();
    System.out.println("Input: " + input);
    final LinkedBlockingQueue<Pair<Object, Integer>> result = new LinkedBlockingQueue<>();


    // open channel
    final String address = (String) input.get("address");
    final Integer port = (Integer) input.get("port");

    controlChannel = channelOpen(address, port);

    if (offloadingTransform != null) {
      offloadingTransform.close();
      offloadingTransform = null;
    }

    map.clear();

    requestId = (Integer) input.get("requestId");


    map.put(controlChannel, new LambdaEventHandler(result));
    handler = (LambdaEventHandler) map.get(controlChannel);

    System.out.println("Open channel: " + controlChannel);

    // load class loader

    if (classLoader == null) {
      System.out.println("Loading jar: " + controlChannel);
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

    byte[] bytes = ByteBuffer.allocate(Integer.BYTES).putInt(requestId).array();

    controlChannel = handshake(bytes, address, port, controlChannel, result);
    handler = (LambdaEventHandler) map.get(controlChannel);

    // Waiting worker init done..
    LOG.info("Waiting worker init or end");

    if (handler == null) {
      LOG.info("handler is null for channel " + controlChannel);
      controlChannel = handshake(bytes, address, port, controlChannel, result);
      handler = (LambdaEventHandler) map.get(controlChannel);
    }

    while (workerInitLatch.getCount() > 0 && endBlockingQueue.isEmpty()) {
      if (!controlChannel.isActive()) {
        controlChannel = handshake(bytes, address, port, controlChannel, result);
        handler = (LambdaEventHandler) map.get(controlChannel);
      }
      try {
        Thread.sleep(1000);
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
    }

    if (workerInitLatch.getCount() == 0) {

      LOG.info("try to print worker spec");
      printSpec(requestId);

      final byte[] addrPortBytes = ByteBuffer.allocate(Integer.BYTES + Integer.BYTES)
        .putInt(executorDataAddrPort)
        .putInt(requestId).array();
      controlChannel.writeAndFlush(new OffloadingMasterEvent(OffloadingMasterEvent.Type.WORKER_INIT_DONE, addrPortBytes, addrPortBytes.length));
      LOG.info("Sending worker init done");

      // final ByteBuf buf2 = dataChannel.alloc().ioBuffer(Integer.BYTES).writeInt(requestId);
      // dataChannel.writeAndFlush(new OffloadingExecutorControlEvent(
      //  OffloadingExecutorControlEvent.Type.ACTIVATE, buf2));

    }

    // ready state
    //opendChannel.writeAndFlush(new OffloadingMasterEvent(OffloadingMasterEvent.Type.READY, new byte[0], 0));


//
//    workerHeartbeatExecutor.shutdown();
//
//    LOG.info("Finishing channels");
//    map.entrySet().forEach(entry -> {
//      entry.getKey().close().awaitUninterruptibly();
//    });
//
//    map.clear();

  }

  private String getMacAddress() {

    final byte[] mac;
    try {
      mac = NetworkInterface.getNetworkInterfaces().nextElement().getHardwareAddress();

      StringBuilder sb = new StringBuilder();
      for (int i = 0; i < mac.length; i++) {
        sb.append(String.format("%02X%s", mac[i], (i < mac.length - 1) ? "-" : ""));
      }
      return sb.toString();
    } catch (SocketException e) {
      e.printStackTrace();
      throw new RuntimeException(e);
    }

    /*
	  try {
      InetAddress localHost = InetAddress.getLocalHost();
      NetworkInterface ni = NetworkInterface.getByInetAddress(localHost);
      byte[] hardwareAddress = ni.getHardwareAddress();

      String[] hexadecimal = new String[hardwareAddress.length];
      for (int i = 0; i < hardwareAddress.length; i++) {
        hexadecimal[i] = String.format("%02X", hardwareAddress[i]);
      }
      String macAddress = String.join("-", hexadecimal);

      return macAddress;
    } catch (final Exception e) {
	    e.printStackTrace();
	    throw new RuntimeException(e);
    }
    */
  }

  private void printSpec(final int requestId) {
    try {
      LOG.info("Worker info" + requestId + " machine identifier " + ComputerIdentifierGenerator.get());
      /* Total number of processors or cores available to the JVM */
      LOG.info("Worker info " + requestId + " available processors (cores): " +
        Runtime.getRuntime().availableProcessors());

      /* Total amount of free memory available to the JVM */
      LOG.info("Worker info " + requestId + " free memory (bytes): " +
        Runtime.getRuntime().freeMemory());

      /* Total amount of free memory available to the JVM */
      // LOG.info("Worker info " + requestId + " mac address" + GetNetworkAddress.GetAddress("mac"));

      LOG.info("Worker info " + requestId + " mac address " + getMacAddress());

      /* This will return Long.MAX_VALUE if there is no preset limit */
      long maxMemory = Runtime.getRuntime().maxMemory();
      /* Maximum amount of memory the JVM will attempt to use */
      LOG.info("Worker info " + requestId + " maximum memory (bytes): " +
        (maxMemory == Long.MAX_VALUE ? "no limit" : maxMemory));

      /* Total memory currently available to the JVM */
      LOG.info("Worker info " + requestId + " total memory available to JVM (bytes): " +
        Runtime.getRuntime().totalMemory());

      RuntimeMXBean runtimeBean = ManagementFactory.getRuntimeMXBean();

      Map<String, String> systemProperties = runtimeBean.getSystemProperties();
      Set<String> keys = systemProperties.keySet();
      for (String key : keys) {
        String value = systemProperties.get(key);
        LOG.info("Worker info " + requestId + "[" + key + "] = " + value);
      }


      InetAddress ip;
      String hostname;
      ip = InetAddress.getLocalHost();
      hostname = ip.getHostName();
      LOG.info("Worker info " + requestId + " current IP address: " + ip);
      LOG.info("Worker info " + requestId + " current Hostname: " + hostname);

    } catch (Exception e) {
      e.printStackTrace();
      throw new RuntimeException(e);
    }
  }

  private void schedule() {
    // cpu heartbeat
    final Channel ochannel = controlChannel;
    workerHeartbeatExecutor = Executors.newSingleThreadScheduledExecutor();
    workerHeartbeatExecutor.scheduleAtFixedRate(() -> {
      CpuInfoExtractor.printNetworkStat(requestId);
      final double cpuLoad = operatingSystemMXBean.getProcessCpuLoad();
      System.out.println("CPU Load: " + cpuLoad);
      final ByteBuf bb = ochannel.alloc().buffer();
      bb.writeDouble(cpuLoad);
      // ochannel.writeAndFlush(new OffloadingMasterEvent(OffloadingMasterEvent.Type.CPU_LOAD, bb));
    }, 2, 2, TimeUnit.SECONDS);
  }

  private void shutdownSchedule() {
	  workerHeartbeatExecutor.shutdown();
  }

	public Object handleRequest(Map<String, Object> input, Context context) {

	  if (context != null) {
      LOG.info("Function memory limit " + context.getMemoryLimitInMB() + " MB");
    }

    if (!input.containsKey("address")) {
      // LOG.info("Worker info " + requestId + " mac address" + GetNetworkAddress.GetAddress("mac"));
      LOG.info("Worker info " + requestId + " mac address" + getMacAddress());
      CpuInfoExtractor.printSpecs(0);
      CpuInfoExtractor.printNetworkStat(0);
      return null;
    }


    final String address = (String) input.get("address");
    final Integer port = (Integer) input.get("port");
	  final String addr =  "/" + address + ":"+ port;

	  if (controlChannel != null) {
	    LOG.info("Remote address control channel" + controlChannel.remoteAddress()
        +  ", addr " + addr);
    }

	  if (controlChannel != null
      && controlChannel.isOpen()
      && controlChannel.isActive()
      && controlChannel.remoteAddress().toString().equals(addr)) {
	    // warmed container!!
      // TODO: check requestId
      LOG.info("Warmed container for request id " + requestId +
        " control channel" + controlChannel);

      // TODO: warm up
      final ByteBuf buf = controlChannel.alloc().ioBuffer(Integer.BYTES).writeInt(requestId);
      controlChannel.writeAndFlush(
        new OffloadingMasterEvent(OffloadingMasterEvent.Type.ACTIVATE, buf));

      // final ByteBuf buf2 = dataChannel.alloc().ioBuffer(Integer.BYTES).writeInt(requestId);
      // dataChannel.writeAndFlush(new OffloadingExecutorControlEvent(
      //  OffloadingExecutorControlEvent.Type.ACTIVATE, buf2));

    } else {
	    LOG.info("Init input " + input + "... control channel " + controlChannel);
	    initialization(input);
    }

    schedule();
	  if (offloadingTransform != null) {
      offloadingTransform.schedule();
    }

    final long sst = System.currentTimeMillis();

    try {
      // wait until end
      System.out.println("Wait deactivation");
      final Integer endFlag = endBlockingQueue.take();

      shutdownSchedule();
      if (offloadingTransform != null) {
        offloadingTransform.shutdownSchedule();
      }

      if (endFlag == 0) {
        System.out.println("end elapsed time: " + (System.currentTimeMillis() - sst));
        try {
          if (controlChannel.isOpen()) {
            controlChannel.writeAndFlush(
              new OffloadingMasterEvent(OffloadingMasterEvent.Type.END, new byte[0], 0)).get();

            if (workerInitLatch.getCount() == 0) {
              // dataChannel.writeAndFlush(
              //  new OffloadingExecutorControlEvent(OffloadingExecutorControlEvent.Type.DEACTIVATE, null)).get();
            }
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
      } else if (endFlag == 1) {
        // Duplicate request termination
        LOG.info("Duplicate request termination ... sending data channel deactive");
//        try {
//          dataChannel.writeAndFlush(
//            new OffloadingExecutorControlEvent(OffloadingExecutorControlEvent.Type.DEACTIVATE, null)).get();
//        } catch (ExecutionException e) {
//          e.printStackTrace();
//          throw new RuntimeException(e);
//        }
      }

      System.out.println("END of invocation: " + (System.currentTimeMillis() - sst));
    } catch (InterruptedException e) {
      e.printStackTrace();
      throw new RuntimeException(e);
    }
    return null;
	}

  public final class LambdaEventHandler implements EventHandler<OffloadingMasterEvent> {


    private final BlockingQueue<Pair<Object, Integer>> result;
    private OffloadingDecoder decoder;

    private long workerFinishTime;

    public LambdaEventHandler(final BlockingQueue<Pair<Object, Integer>> result) {
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
            System.out.println("Before OffloadingTransform: ");
            final DataInputStream dis = new DataInputStream(bis);
            offloadingTransform = OffloadingExecutor.decode(dis);
            decoder = new OffloadingExecutorInputDecoder();
            outputEncoder = new MiddleOffloadingOutputEncoder();

            Thread.currentThread().setContextClassLoader(classLoader);
            // ObjectInputStream ois = new ExternalJarObjectInputStream(classLoader, bis);
            // offloadingTransform = (OffloadingTransform) ois.readObject();
            System.out.println("After OffloadingTransform: ");
            // decoder = (OffloadingDecoder) ois.readObject();
            // outputEncoder = (OffloadingEncoder) ois.readObject();

            System.out.println("OffloadingTransform: " + offloadingTransform);

            // ois.close();
            bis.close();
            byteBuf.release();
          } catch (IOException e) {
            e.printStackTrace();
            throw new RuntimeException(e);
          }

          outputCollector = new LambdaOutputHandler(result);

          LOG.info("Before offloading prepare, stageTaskMap size: " + stageTaskMap.size());

          // TODO: OffloadingTransform that receives data from parent tasks should register its id
          // to lambdaEventHandlerMap
          offloadingTransform.prepare(
            new LambdaRuntimeContext(lambdaEventHandlerMap, this, isSf,
              nameServerAddr, nameServerPort, newExecutorId, controlChannel, throttleRate,
              testing, stageTaskMap, requestId, new ControlMessageFromExecutorHandler()), outputCollector);

          LOG.info("End of offloading prepare");

          workerFinishTime = System.currentTimeMillis();
          // executorDataAddrPort = offloadingTransform.getDataChannelPort();
          // dataChannel = offloadingTransform.getDataChannel();
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


            /*
            final ByteBufOutputStream bos = new ByteBufOutputStream(
              offloadingTransform.getDataChannel().alloc().ioBuffer());

            bos.writeUTF(taskId);
            bos.close();

            LOG.info("Send task ready for " + taskId + " to " + offloadingTransform.getDataChannel());

            offloadingTransform.getDataChannel()
              .writeAndFlush(new OffloadingExecutorControlEvent(
                OffloadingExecutorControlEvent.Type.TASK_READY, bos.buffer()));
                */


            offloadingTransform.onData(data, null);
            outputCollector.hasDataReceived = false;
            //System.out.println("Data processing done: " + (System.currentTimeMillis() - st));
            dataProcessingCnt += 1;

            nemoEvent.getByteBuf().release();



            final ByteBufOutputStream bo2 = new ByteBufOutputStream(
              controlChannel.alloc().ioBuffer());

            bo2.writeUTF(taskId);
            bo2.close();

            controlChannel.writeAndFlush(new OffloadingMasterEvent(
             OffloadingMasterEvent.Type.TASK_READY, bo2.buffer()));

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
        case DUPLICATE_REQUEST_TERMIATION: {
          System.out.println("Duplicate request termination");
          endBlockingQueue.add(1);
          break;
        }
        case END:
          // send result
          System.out.println("Offloading end");
          // if (offloadingTransform != null) {
          //  offloadingTransform.close();
          // }
          nemoEvent.getByteBuf().release();
          endBlockingQueue.add(0);
          // end of event
          // update handler
          break;
        default:
          throw new RuntimeException("Invalid type " + nemoEvent.getType());
      }
    }
  }

  public final class ControlMessageFromExecutorHandler extends SimpleChannelInboundHandler<TaskControlMessage> {

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, TaskControlMessage msg) throws Exception {
      switch (msg.type) {
        case DEACTIVATE_LAMBDA: {
          System.out.println("Offloading end from executor for deactivate");
          Thread.sleep(200);
          while (offloadingTransform.hasRemainingEvent()) {
            LOG.info("Waiting for handling remaining events to deactivate");
            Thread.sleep(20);
          }
          endBlockingQueue.add(0);
          break;
        }
        default: {
          throw new RuntimeException("Noit supported");
        }
      }
    }

    @Override
    public void channelActive(final ChannelHandlerContext ctx) {
      // channelGroup.add(ctx.channel());
      // outputWriterFlusher.registerChannel(ctx.channel());
    }

    @Override
    public void channelInactive(final ChannelHandlerContext ctx) {
      // channelGroup.remove(ctx.channel());
      // outputWriterFlusher.removeChannel(ctx.channel());
      LOG.info("Channel closed !! " + ctx.channel());
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
