package org.apache.nemo.common;

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
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.nemo.common.coder.DecoderFactory;
import org.apache.nemo.common.coder.EncoderFactory;
import org.apache.nemo.common.ir.OutputCollector;
import org.apache.nemo.common.ir.vertex.OperatorVertex;
import org.apache.nemo.common.ir.vertex.transform.Transform;
import org.apache.nemo.common.punctuation.Watermark;

import java.io.*;
import java.net.InetSocketAddress;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.*;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class OffloadingHandler {

	private static final Logger LOG = LoggerFactory.getLogger(OffloadingHandler.class);
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

  private final ConcurrentMap<Channel, EventHandler<NemoEvent>> map;

  private List<String> serializedVertices;
  private List<Transform> transforms;

  // current states of lambda
  private LambdaStatus status;

  private final Callable<ClassLoader> classLoaderCallable;

  private EncoderFactory outputEncoderFactory;

  private LambdaOutputHandler outputCollector;

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

	private final List<Transform> buildTransformChain(final List<String> serializedTransforms,
                                                    final ClassLoader classLoader) {
    final List<Transform> vertices = serializedTransforms.stream().map(str -> {
      return (Transform) SerializeUtils.deserializeFromString(str, classLoader);
    }).collect(Collectors.toList());

    return vertices;
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

	public Object handleRequest(Map<String, Object> input) {
	  final long st = System.currentTimeMillis();

		System.out.println("Input: " + input);
    final LinkedBlockingQueue<Pair<Object, Integer>> result = new LinkedBlockingQueue<>();

    // open channel
    Channel opendChannel = null;
    for (final Map.Entry<Channel, EventHandler<NemoEvent>> entry : map.entrySet()) {
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
        classLoader = classLoaderCallable.call();
      } catch (Exception e) {
        e.printStackTrace();
        throw new RuntimeException(e);
      }
      status = LambdaStatus.READY;
      LOG.info("Create class loader: {}");
    }

    // write handshake
    System.out.println("Write handshake: " + (System.currentTimeMillis() - st));
    opendChannel.writeAndFlush(new NemoEvent(NemoEvent.Type.CLIENT_HANDSHAKE, new byte[0], 0));

    // ready state
    //opendChannel.writeAndFlush(new NemoEvent(NemoEvent.Type.READY, new byte[0], 0));

    final LambdaEventHandler handler = (LambdaEventHandler) map.get(opendChannel);

		final List<ChannelFuture> futures = new LinkedList<>();

		// send result
    while (handler.endBlockingQueue.isEmpty()) {
      while (result.peek() != null) {
        final Object data = result.poll();
        final ByteBuf byteBuf = opendChannel.alloc().ioBuffer();
        byteBuf.writeInt(NemoEvent.Type.RESULT.ordinal());
        final ByteBufOutputStream bis = new ByteBufOutputStream(byteBuf);
        final EncoderFactory.Encoder encoder;
        try {
          encoder = outputEncoderFactory.create(bis);
          encoder.encode(data);
          bis.close();
        } catch (IOException e) {
          e.printStackTrace();
          throw new RuntimeException(e);
        }

        System.out.println("Write result");
        futures.add(opendChannel.writeAndFlush(
          new NemoEvent(NemoEvent.Type.RESULT, byteBuf)));
      }

      try {
        Thread.sleep(10);
      } catch (InterruptedException e) {
        e.printStackTrace();
        throw new RuntimeException(e);
      }
    }

    final long sst = System.currentTimeMillis();

    while (result.peek() != null) {
      final Pair<Object, Integer> data = result.poll();
      final ByteBuf byteBuf = opendChannel.alloc().ioBuffer();
      byteBuf.writeInt(NemoEvent.Type.RESULT.ordinal());
      final ByteBufOutputStream bis = new ByteBufOutputStream(byteBuf);
      final EncoderFactory.Encoder encoder;
      try {
        encoder = outputEncoderFactory.create(bis);
        encoder.encode(data.left());
        bis.writeInt(data.right());
        bis.close();
      } catch (IOException e) {
        e.printStackTrace();
        throw new RuntimeException(e);
      }

      System.out.println("Write result");
      futures.add(opendChannel.write(
        new NemoEvent(NemoEvent.Type.RESULT, byteBuf)));
    }

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
        System.out.println("end");
        try {
          opendChannel.writeAndFlush(new NemoEvent(NemoEvent.Type.END, new byte[0], 0)).get();
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

  final class LambdaEventHandler implements EventHandler<NemoEvent> {

    private WindowedValue sideInput;
    private LambdaDecoderFactory sideInputDecoderFactory;
    private LambdaDecoderFactory mainInputDecoderFactory;
    private DecoderFactory gbkDecoderFactory;

    private final BlockingQueue<Integer> endBlockingQueue = new LinkedBlockingQueue<>();
    private final Channel opendChannel;
    private final BlockingQueue<Pair<Object, Integer>> result;
    private DecoderFactory decoderFactory;

    private long workerFinishTime;

    public LambdaEventHandler(final Channel opendChannel,
                              final BlockingQueue<Pair<Object, Integer>> result) {
      this.opendChannel = opendChannel;
      this.result = result;
    }

    @Override
    public synchronized void onNext(final NemoEvent nemoEvent) {
      switch (nemoEvent.getType()) {
        case WORKER_INIT: {
          System.out.println("Worker init...");
          final long st = System.currentTimeMillis();
          // load transforms
          final ByteBuf byteBuf = nemoEvent.getByteBuf();
          ByteBufInputStream bis = new ByteBufInputStream(byteBuf);
          //System.out.println("Serialized transforms size: " + bytes.length);
          //ByteArrayInputStream bis = new ByteArrayInputStream(bytes);
          try {
            ObjectInputStream ois = new ExternalJarObjectInputStream(classLoader, bis);
            offloadingTransform = (OffloadingTransform) ois.readObject();
            decoderFactory = (DecoderFactory) ois.readObject();
            outputEncoderFactory = (EncoderFactory) ois.readObject();

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
            new LambdaRuntimeContext(
              new OperatorVertex(offloadingTransform)), outputCollector);
          System.out.println("End of worker init: " + (System.currentTimeMillis() - st));

          workerFinishTime = System.currentTimeMillis();
          break;
        }
        case DATA: {
          final long st = System.currentTimeMillis();
          System.out.println("Worker init -> data time: " + (st - workerFinishTime));

          final ByteBufInputStream bis = new ByteBufInputStream(nemoEvent.getByteBuf());
          DecoderFactory.Decoder decoder;
          try {
            decoder = decoderFactory.create(bis);
          } catch (final IOException e) {
            e.printStackTrace();
            throw new RuntimeException();
          }

          try {
            final Object data = decoder.decode();
            final int dataId = bis.readInt();
            outputCollector.setDataId(dataId);

            //System.out.println("Receive data: " + data);
            offloadingTransform.onData(data);
          } catch (IOException e) {
            if (e.getMessage().contains("EOF")) {
              System.out.println("eof!");
              break;
            } else {
              e.printStackTrace();
              throw new RuntimeException(e);
            }
          }

          System.out.println("Data processing done: " + (System.currentTimeMillis() - st));

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

  final class ChainOutputHandler implements OutputCollector {
	  final Transform nextTransform;
	  public ChainOutputHandler(final Transform transform) {
	    this.nextTransform = transform;
    }

    @Override
    public void emit(Object output) {
      nextTransform.onData(output);
    }

    @Override
    public void emitWatermark(Watermark watermark) {
      nextTransform.onWatermark(watermark);
    }

    @Override
    public void emit(String dstVertexId, Object output) {

    }
  }

	final class LambdaOutputHandler  implements OutputCollector {

	  private final BlockingQueue<Pair<Object, Integer>> result;
	  private int dataId;

	  public LambdaOutputHandler(final BlockingQueue<Pair<Object, Integer>> result) {
	    this.result = result;
    }

    void setDataId(final int id) {
	    dataId = id;
    }

    @Override
    public void emit(Object output) {
      System.out.println("Emit output: " + output.toString());
      result.add(Pair.of(output, dataId));
    }

    @Override
    public void emitWatermark(Watermark watermark) {

    }

    @Override
    public void emit(String dstVertexId, Object output) {

    }
  }
}
