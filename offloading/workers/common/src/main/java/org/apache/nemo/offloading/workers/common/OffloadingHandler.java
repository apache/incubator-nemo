package org.apache.nemo.offloading.workers.common;

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
import org.apache.log4j.Logger;
import org.apache.nemo.offloading.common.*;

import java.io.*;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
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

  private final ConcurrentMap<Channel, EventHandler<OffloadingEvent>> map;

  private List<String> serializedVertices;

  // current states of lambda
  private LambdaStatus status;

  private final Callable<ClassLoader> classLoaderCallable;

  private OffloadingEncoder outputEncoder;

  private LambdaOutputHandler outputCollector;

  private int dataProcessingCnt = 0;

	public OffloadingHandler(final Callable<ClassLoader> classLoaderCallable) {
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
    this.classLoaderCallable = classLoaderCallable;
	}

  private Channel channelOpen(final Map<String, Object> input) {
    // 1) connect to the VM worker
    final String address = (String) input.get("address");
    final Integer port = (Integer) input.get("port");

    final ChannelFuture channelFuture;
    channelFuture = clientBootstrap.connect(new InetSocketAddress(address, port));
    channelFuture.awaitUninterruptibly();
    assert channelFuture.isDone();
    if (!channelFuture.isSuccess()) {
      final StringBuilder sb = new StringBuilder("A connection failed at Source - ");
      sb.append(channelFuture.cause());
      throw new RuntimeException(sb.toString());
    }
    final Channel opendChannel = channelFuture.channel();
    return opendChannel;
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

    System.out.println("Write result");
    futures.add(opendChannel.writeAndFlush(
      new OffloadingEvent(OffloadingEvent.Type.RESULT, byteBuf)));
  }

	public Object handleRequest(Map<String, Object> input) {
	  final long st = System.currentTimeMillis();

		System.out.println("Input: " + input);
    final LinkedBlockingQueue<Pair<Object, Integer>> result = new LinkedBlockingQueue<>();

    // open channel
    Channel opendChannel = null;
    for (final Map.Entry<Channel, EventHandler<OffloadingEvent>> entry : map.entrySet()) {
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

    if (opendChannel == null) {
      opendChannel = channelOpen(input);
    }
    map.put(opendChannel, new LambdaEventHandler(opendChannel, result));

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
    byte[] bytes = ByteBuffer.allocate(4).putInt(dataProcessingCnt).array();
    opendChannel.writeAndFlush(new OffloadingEvent(OffloadingEvent.Type.CLIENT_HANDSHAKE, bytes, bytes.length));

    // ready state
    //opendChannel.writeAndFlush(new OffloadingEvent(OffloadingEvent.Type.READY, new byte[0], 0));

    final LambdaEventHandler handler = (LambdaEventHandler) map.get(opendChannel);

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
          opendChannel.writeAndFlush(new OffloadingEvent(OffloadingEvent.Type.END, new byte[0], 0)).get();
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


    return null;
	}

  final class LambdaEventHandler implements EventHandler<OffloadingEvent> {

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
    public synchronized void onNext(final OffloadingEvent nemoEvent) {
      switch (nemoEvent.getType()) {
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
            offloadingTransform = (OffloadingTransform) ois.readObject();
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
          offloadingTransform.prepare(
            new LambdaRuntimeContext(), outputCollector);
          System.out.println("End of worker init: " + (System.currentTimeMillis() - st));

          workerFinishTime = System.currentTimeMillis();
          break;
        }
        case DATA: {
          final long st = System.currentTimeMillis();
          Thread.currentThread().setContextClassLoader(classLoader);
          System.out.println("Worker init -> data time: " + (st - workerFinishTime) +
            " databytes: " + nemoEvent.getByteBuf().readableBytes());


          final ByteBufInputStream bis = new ByteBufInputStream(nemoEvent.getByteBuf());
          try {
            final Object data = decoder.decode(bis);
            final int dataId = bis.readInt();
            outputCollector.setDataId(dataId);

            System.out.println("Read data " + dataId);

            //System.out.println("Receive data: " + data);
            offloadingTransform.onData(data);

            if (!outputCollector.hasDataReceived) {
              outputCollector.emit(NoResult.INSTANCE);
            }

            outputCollector.hasDataReceived = false;

          } catch (IOException e) {
            if (e.getMessage().contains("EOF")) {
              System.out.println("eof!");
            } else {
              e.printStackTrace();
              throw new RuntimeException(e);
            }
          }

          System.out.println("Data processing done: " + (System.currentTimeMillis() - st));
          dataProcessingCnt += 1;

          nemoEvent.getByteBuf().release();

          break;
        }
        case END:
          // send result
          System.out.println("Offloading end");
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
      System.out.println("Emit output of data " + dataId);
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
