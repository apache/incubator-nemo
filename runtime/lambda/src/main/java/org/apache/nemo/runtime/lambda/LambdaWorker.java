package org.apache.nemo.runtime.lambda;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.S3Object;
import io.netty.bootstrap.Bootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.util.concurrent.DefaultThreadFactory;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.commons.lang3.SerializationUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.nemo.common.EventHandler;
import org.apache.nemo.common.NemoEvent;
import org.apache.nemo.common.NettyChannelInitializer;
import org.apache.nemo.common.coder.DecoderFactory;
import org.apache.nemo.common.ir.OutputCollector;
import org.apache.nemo.common.ir.vertex.OperatorVertex;
import org.apache.nemo.common.punctuation.Watermark;

import java.io.*;
import java.net.InetSocketAddress;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.*;
import java.util.stream.Collectors;

public class LambdaWorker implements RequestHandler<Map<String, Object>, Object> {

	private static final Logger LOG = LogManager.getLogger(LambdaWorker.class);
	//private static final OutputSender sender = new OutputSender("18.182.129.182", 20312);
	private static final AmazonS3 s3Client = AmazonS3ClientBuilder.standard()
    .withClientConfiguration(new ClientConfiguration().withMaxConnections(10)).build();

	private static final String BUCKET_NAME = "nemo-serverless";
	private static final String PATH = "/tmp/nexmark-0.2-SNAPSHOT-shaded.jar";
	//private static final String PATH = "/tmp/shaded.jar";
	private URLClassLoader classLoader = null;
	private OperatorVertex headVertex = null;

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
  private List<OperatorVertex> vertices;

  // current states of lambda
  private LambdaStatus status;

	private void createClassLoader() {
		// read jar file
		final S3Object result = s3Client.getObject(BUCKET_NAME, "jars/nexmark-0.2-SNAPSHOT-shaded.jar");
		//final S3Object result = s3Client.getObject(BUCKET_NAME, "jars/shaded.jar");
		if (!Files.exists(Paths.get(PATH))) {
			LOG.info("Copying file...");
			final InputStream in = result.getObjectContent();
			try {
				Files.copy(in, Paths.get(PATH));
			} catch (IOException e) {
				e.printStackTrace();
				throw new RuntimeException(e);
			}
		}

		try {
			final URL[] urls = new URL[1];
			final File f = new File(PATH);
      urls[0] = f.toURI().toURL();
			LOG.info("File: {}, {}", f.toPath(), urls[0]);
			classLoader = new URLClassLoader(urls, this.getClass().getClassLoader());
		} catch (MalformedURLException e) {
			e.printStackTrace();
			throw new RuntimeException(e);
		}
  }

	public LambdaWorker() {
		LOG.info("Handler is created! {}", this);
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
	}

	private final List<OperatorVertex> buildOperatorChain(final List<String> serializedVertices, final ClassLoader classLoader) {
    final List<OperatorVertex> vertices = serializedVertices.stream().map(str -> {
      return (OperatorVertex) SerializeUtils.deserializeFromString(str, classLoader);
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

	@Override
	public Object handleRequest(Map<String, Object> input, Context context) {
		System.out.println("Input: " + input);
    final List<String> result = new ArrayList<>();

    // open channel
    Channel opendChannel = null;
    for (final Map.Entry<Channel, EventHandler<NemoEvent>> entry : map.entrySet()) {
      final Channel channel = entry.getKey();
      if (!channel.isOpen()) {
        channel.close();
        map.remove(channel);
      } else {
        opendChannel = channel;
        break;
      }
    }

    if (opendChannel == null) {
      opendChannel = channelOpen(input);
      map.put(opendChannel, new LambdaEventHandler(opendChannel, result));
    }

    System.out.println("Open channel: " + opendChannel);
    // write handshake
    System.out.println("Write handshake");
    opendChannel.writeAndFlush(new NemoEvent(NemoEvent.Type.CLIENT_HANDSHAKE, new byte[0], 0));

    // load class loader
		if (status.equals(LambdaStatus.INIT)) {
      createClassLoader();
      status = LambdaStatus.READY;
      LOG.info("Create class loader: {}", classLoader);
    }

    final LambdaEventHandler handler = (LambdaEventHandler) map.get(opendChannel);
    try {
      // wait until end
      System.out.println("Wait end flag");
      final Integer endFlag = handler.endBlockingQueue.take();
    } catch (InterruptedException e) {
      e.printStackTrace();
      throw new RuntimeException(e);
    }

    if (result.size() > 0) {
      final byte[] bytes = result.toString().getBytes();
      final ChannelFuture future =
        opendChannel.writeAndFlush(
          new NemoEvent(NemoEvent.Type.RESULT, bytes, bytes.length));
      try {
        future.get();
      } catch (InterruptedException e) {
        e.printStackTrace();
      } catch (ExecutionException e) {
        e.printStackTrace();
      }
    }

    //final Decoder object  = (T)ois.readObject();
		//ois.close();
		//return object;

    return null;
    //return result.toString();

	}

  final class LambdaEventHandler implements EventHandler<NemoEvent> {

    private WindowedValue sideInput;
    private LambdaDecoderFactory sideInputDecoderFactory;
    private LambdaDecoderFactory mainInputDecoderFactory;
    private DecoderFactory gbkDecoderFactory;

    private final BlockingQueue<Integer> endBlockingQueue = new LinkedBlockingQueue<>();
    private final Channel opendChannel;
    private final List<String> result;

    public LambdaEventHandler(final Channel opendChannel,
                              final List<String> result) {
      this.opendChannel = opendChannel;
      this.result = result;
    }

    @Override
    public synchronized void onNext(final NemoEvent nemoEvent) {
      switch (nemoEvent.getType()) {
        /*
        case JAR: {
          // load jar
          try {
            final FileOutputStream fc = new FileOutputStream(PATH);
            fc.write(nemoEvent.getBytes());
            fc.close();
            createClassLoader();
            status = LambdaStatus.READY;
            waitJarLoad.countDown();
          } catch (FileNotFoundException e) {
            e.printStackTrace();
            throw new RuntimeException(e);
          } catch (IOException e) {
            e.printStackTrace();
            throw new RuntimeException(e);
          }
          break;
        }
        */
        case VERTICES: {
          // load vertices
          final byte[] bytes = nemoEvent.getBytes();
          ByteArrayInputStream bis = new ByteArrayInputStream(bytes);
          List<String> serializedV;
          try {
            ObjectInputStream ois = new ObjectInputStream(bis);
            serializedV = (List<String>) ois.readObject();
          } catch (IOException e) {
            e.printStackTrace();
            throw new RuntimeException(e);
          } catch (ClassNotFoundException e) {
            e.printStackTrace();
            throw new RuntimeException(e);
          }

          if (serializedVertices == null || !serializedVertices.equals(serializedV)) {
            System.out.println("Serialize vertices");
            serializedVertices = serializedV;
            vertices = buildOperatorChain(serializedVertices, classLoader);
            headVertex = vertices.get(0);

            // connect vertices
            for (int i = 0; i < vertices.size() - 1; i++) {
              vertices.get(i).getTransform().prepare(
                new LambdaRuntimeContext(vertices.get(i)), new ChainOutputHandler(vertices.get(i+1)));
            }
          }

          final OutputCollector outputCollector = new LambdaOutputHandler(result);

          final OperatorVertex finalVertex = vertices.get(vertices.size() - 1);
          finalVertex.getTransform().prepare(new LambdaRuntimeContext(finalVertex), outputCollector);
          break;
        }
        case SIDE: { // query 7
          // receive side input
          System.out.println("Receive side");
          final ByteArrayInputStream bis = new ByteArrayInputStream(nemoEvent.getBytes());
          sideInputDecoderFactory =
            SerializeUtils.deserialize(bis, classLoader);
          try {
            final DecoderFactory.Decoder sideInputDecoder = sideInputDecoderFactory.create(bis);
            sideInput = (WindowedValue) sideInputDecoder.decode();
            headVertex.getTransform().onData(sideInput);
          } catch (IOException e) {
            e.printStackTrace();
            throw new RuntimeException(e);
          }
          break;
        }
        case MAIN: { // query 7
          System.out.println("Receive main ");
          // receive main input
          if (sideInput == null) {
            throw new IllegalStateException("SideInput should not be null");
          }

          final ByteArrayInputStream bis = new ByteArrayInputStream(nemoEvent.getBytes());
          mainInputDecoderFactory =
            SerializeUtils.deserialize(bis, classLoader);
          try {
            final DecoderFactory.Decoder mainInputDecoder = mainInputDecoderFactory.create(bis);
            WindowedValue mainInput = null;
            int cnt = 0;
            while (true) {
              try {
                mainInput = (WindowedValue) mainInputDecoder.decode();
                //handler.processMainAndSideInput(mainInput, sideInput, outputCollector);
                headVertex.getTransform().onData(mainInput);
                cnt += 1;
              } catch (final IOException e) {
                if (e.getMessage().contains("EOF")) {
                  System.out.println("Cnt: " + cnt + ", eof!");
                } else {
                  System.out.println("Cnt: " + cnt + "Windowed value: " + mainInput + ", sideInput: " + sideInput);
                  throw e;
                }
                break;
              }
            }

            sideInput = null;
            // send result
            endBlockingQueue.add(1);
          } catch (IOException e) {
            e.printStackTrace();
            throw new RuntimeException(e);
          }
          break;
        }
        case GBK_START: { // query 8
          System.out.println("Start gbk");
          final ByteArrayInputStream bis = new ByteArrayInputStream(nemoEvent.getBytes());
          gbkDecoderFactory =
            SerializeUtils.deserialize(bis, classLoader);
          break;
        }
        case GBK: {// query 8
          // TODO
          //System.out.println("Receive gbk data");
          final ByteArrayInputStream bis = new ByteArrayInputStream(nemoEvent.getBytes());
          try {
            final DecoderFactory.Decoder gbkDecoder = gbkDecoderFactory.create(bis);
            WindowedValue mainInput = null;
            int cnt = 0;
            while (true) {
              try {
                mainInput = (WindowedValue) gbkDecoder.decode();
                //handler.processMainAndSideInput(mainInput, sideInput, outputCollector);
                headVertex.getTransform().onData(mainInput);
                cnt += 1;
              } catch (final IOException e) {
                if (e.getMessage().contains("EOF")) {
                  //System.out.println("Cnt: " + cnt + ", eof!");
                } else {
                  System.out.println("Cnt: " + cnt + "Windowed value: " + mainInput + ", sideInput: " + sideInput);
                  throw e;
                }
                break;
              }
            }
          } catch (IOException e) {
            e.printStackTrace();
            throw new RuntimeException(e);
          }
          break;
        }
        case END:
          // send result
          final byte[] bytes = result.toString().getBytes();
          final ChannelFuture future =
            opendChannel.writeAndFlush(
              new NemoEvent(NemoEvent.Type.RESULT, bytes, bytes.length));
          endBlockingQueue.add(1);
          // end of event
          // update handler
          break;
        case WARMUP_END:
          System.out.println("Warmup end");
          endBlockingQueue.add(1);
          break;
      }
    }
  }

  final class ChainOutputHandler implements OutputCollector {
	  final OperatorVertex nextVertex;
	  public ChainOutputHandler(final OperatorVertex nextVertex) {
	    this.nextVertex = nextVertex;
    }

    @Override
    public void emit(Object output) {
      nextVertex.getTransform().onData(output);
    }

    @Override
    public void emitWatermark(Watermark watermark) {
      nextVertex.getTransform().onWatermark(watermark);
    }

    @Override
    public void emit(String dstVertexId, Object output) {

    }
  }

	final class LambdaOutputHandler  implements OutputCollector {

	  private final List<String> result;

	  public LambdaOutputHandler(final List<String> result) {
	    this.result = result;
    }

    @Override
    public void emit(Object output) {
      System.out.println("Emit output: " + output.toString());
      result.add(output.toString());
    }

    @Override
    public void emitWatermark(Watermark watermark) {

    }

    @Override
    public void emit(String dstVertexId, Object output) {

    }
  }
}
