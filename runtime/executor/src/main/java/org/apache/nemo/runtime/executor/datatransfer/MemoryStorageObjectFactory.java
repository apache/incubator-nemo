package org.apache.nemo.runtime.executor.datatransfer;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.services.lambda.AWSLambda;
import com.amazonaws.services.lambda.AWSLambdaClientBuilder;
import com.amazonaws.services.lambda.model.InvokeRequest;
import com.amazonaws.services.lambda.model.InvokeResult;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.group.ChannelGroup;
import io.netty.channel.group.DefaultChannelGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.util.concurrent.DefaultThreadFactory;
import io.netty.util.concurrent.GlobalEventExecutor;
import org.apache.commons.lang.SerializationUtils;
import org.apache.nemo.common.*;
import org.apache.nemo.common.coder.DecoderFactory;
import org.apache.nemo.common.coder.EncoderFactory;
import org.apache.nemo.runtime.executor.data.SerializerManager;
import org.apache.nemo.runtime.lambda.LambdaDecoderFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

public final class MemoryStorageObjectFactory implements StorageObjectFactory {
  private static final Logger LOG = LoggerFactory.getLogger(MemoryStorageObjectFactory.class.getName());

  public static final MemoryStorageObjectFactory INSTACE = new MemoryStorageObjectFactory();

  private final ConcurrentMap<String, ConcurrentLinkedQueue<MemoryStorageObject>> prefixAndObjectMap;
  private final ConcurrentMap<String, AtomicInteger> prefixAndSizeMap;

  private static final int SERVER_BOSS_NUM_THREADS = 3;
  private static final int SERVER_WORKER_NUM_THREADS = 10;
  private static final String CLASS_NAME = MemoryStorageObjectFactory.class.getName();
  private static final String ADDRESS = "172.31.21.224";
  private static final String PUBLIC_ADDRESS = "52.194.185.159";
  private static final int PORT = 20332;

  private final ChannelGroup serverChannelGroup = new DefaultChannelGroup(GlobalEventExecutor.INSTANCE);
  private final EventLoopGroup serverBossGroup;
  private final EventLoopGroup serverWorkerGroup;
  private final Channel acceptor;
  private final AWSLambda awsLambda;

  private final NemoEventHandler nemoEventHandler;


  private MemoryStorageObjectFactory() {
    this.prefixAndObjectMap = new ConcurrentHashMap<>();
    this.prefixAndSizeMap = new ConcurrentHashMap<>();
    this.awsLambda = AWSLambdaClientBuilder.standard().withClientConfiguration(
      new ClientConfiguration().withMaxConnections(150)).build();
    this.serverBossGroup = new NioEventLoopGroup(SERVER_BOSS_NUM_THREADS,
        new DefaultThreadFactory(CLASS_NAME + "SourceServerBoss"));
    this.serverWorkerGroup = new NioEventLoopGroup(SERVER_WORKER_NUM_THREADS,
        new DefaultThreadFactory(CLASS_NAME + "SourceServerWorker"));
    this.nemoEventHandler = new NemoEventHandler();
    final ServerBootstrap serverBootstrap = new ServerBootstrap();
    serverBootstrap.group(this.serverBossGroup, this.serverWorkerGroup)
      .channel(NioServerSocketChannel.class)
      .childHandler(new NettyChannelInitializer(
        new NettyServerSideChannelHandler(serverChannelGroup, nemoEventHandler)))
      .option(ChannelOption.SO_BACKLOG, 128)
      .option(ChannelOption.SO_REUSEADDR, true)
      .childOption(ChannelOption.SO_KEEPALIVE, true);
    try {
      this.acceptor = serverBootstrap.bind(
        new InetSocketAddress(ADDRESS, PORT)).sync().channel();
    } catch (InterruptedException e) {
      e.printStackTrace();
      throw new RuntimeException(e);
    }
  }

  @Override
  public StorageObject newInstance(String prefix,
                                   String suffix,
                                   int partition,
                                   byte[] encodedDecoderFactory,
                                   EncoderFactory encoderFactory) {
    final MemoryStorageObject memoryStorageObject =
      new MemoryStorageObject(prefix+suffix, partition, encodedDecoderFactory, encoderFactory);
    prefixAndObjectMap.putIfAbsent(prefix, new ConcurrentLinkedQueue<>());
    prefixAndObjectMap.get(prefix).add(memoryStorageObject);
    prefixAndSizeMap.putIfAbsent(prefix, new AtomicInteger(0));
    prefixAndSizeMap.get(prefix).getAndIncrement();
    return memoryStorageObject;
  }

  public SideInputProcessor sideInputProcessor(SerializerManager serializerManager,
                                               String edgeId) {
    return new MemorySideInputProcessor(serializerManager, edgeId);
  }

  final class MemoryStorageObject implements StorageObject {
    private final EncoderFactory.Encoder encoder;
    private final DirectByteArrayOutputStream outputStream;
    public final String fname;
    private final AtomicBoolean closed = new AtomicBoolean(false);
    private boolean finished = false;
    private final int partition;
    private final String prefix;
    int cnt = 0;
    public MemoryStorageObject(final String prefix, final int partition,
                               final byte[] encodedDecoderFactory,
                               final EncoderFactory encoderFactory) {
      this.prefix = prefix;
      this.fname = prefix + "-" + partition;
      this.partition = partition;
      this.outputStream = new DirectByteArrayOutputStream();
      try {
        outputStream.write(encodedDecoderFactory);
        this.encoder = encoderFactory.create(outputStream);
      } catch (IOException e) {
        e.printStackTrace();
        throw new RuntimeException(e);
      }
    }

    @Override
    public void encode(Object object) {
      try {
        cnt += 1;
        encoder.encode(object);
      } catch (IOException e) {
        e.printStackTrace();
        throw new RuntimeException(e);
      }
    }

    @Override
    public void close() {
      if (closed.compareAndSet(false, true)) {
        try {
          outputStream.close();
          finished = true;
        } catch (IOException e) {
          e.printStackTrace();
          throw new RuntimeException(e);
        }
      }
    }

    @Override
    public int getPartition() {
      return partition;
    }

    @Override
    public String getPrefix() {
      return prefix;
    }

    @Override
    public String toString() {
      return fname;
    }
  }

  final class NemoEventHandler implements EventHandler<Pair<Channel,NemoEvent>> {

    private final BlockingQueue<Pair<Channel,NemoEvent>> handshakeQueue;

    NemoEventHandler() {
      this.handshakeQueue = new LinkedBlockingQueue<>();
    }

    @Override
    public void onNext(Pair<Channel,NemoEvent> nemoEvent) {
      final NemoEvent event = (NemoEvent) nemoEvent.right();
      switch (event.getType()) {
        case CLIENT_HANDSHAKE:
          handshakeQueue.add(nemoEvent);
          break;
        default:
          throw new IllegalStateException("Illegal type: " + event.getType().name());
      }
    }
  }

  final class MemorySideInputProcessor implements SideInputProcessor {
    private final ExecutorService executorService = Executors.newCachedThreadPool();
    private final EncoderFactory encoderFactory;
    private EncoderFactory.Encoder encoder = null;
    private final DecoderFactory decoderFactory;
    private final byte[] sideInputDecodedFactory;

    MemorySideInputProcessor(final SerializerManager serializerManager,
                             final String edgeId) {
      this.encoderFactory = ((NemoEventEncoderFactory) serializerManager.getSerializer(edgeId)
        .getEncoderFactory()).getValueEncoderFactory();
      this.decoderFactory = new LambdaDecoderFactory(
        ((NemoEventDecoderFactory) serializerManager.getSerializer(edgeId)
          .getDecoderFactory()).getValueDecoderFactory());
      this.sideInputDecodedFactory = SerializationUtils.serialize(decoderFactory);
    }

    @Override
    public Object processSideAndMainInput(Object output, String sideInputKey) {
      // Invoke lambdas

      final long startTime = System.currentTimeMillis();

      final ConcurrentLinkedQueue<MemoryStorageObject> queue = prefixAndObjectMap.get(sideInputKey);
      final int size = prefixAndSizeMap.get(sideInputKey).get();

      LOG.info("Side Input Key: {}", sideInputKey);
      final List<MemoryStorageObject> list = new ArrayList<>(size+5);
      while (!queue.isEmpty()) {
        final MemoryStorageObject object = queue.poll();
        list.add(object);
      }

      LOG.info("MemoryStorageObject size: {}", list.size());

      final List<Future<InvokeResult>> futures = new ArrayList<>(list.size());
      // 0. Trigger lambdas
      for (int i = 0; i < list.size(); i++) {
        futures.add(executorService.submit(() -> {
          // Trigger lambdas
          final InvokeRequest request = new InvokeRequest()
            .withFunctionName(AWSUtils.SIDEINPUT_LAMBDA_NAME2)
            .withPayload(String.format("{\"address\":\"%s\", \"port\": %d}",
              PUBLIC_ADDRESS, PORT));
          return awsLambda.invoke(request);
        }));
      }

      LOG.info("End of invocation of lambdas: {}, {}", list.size(),
        System.currentTimeMillis() - startTime);
      final List<Channel> channels = new ArrayList<>(list.size());

      // 1. lambda initialized
      for (int i = 0; i < list.size(); i++) {
        try {
          channels.add(nemoEventHandler.handshakeQueue.take().left());
        } catch (InterruptedException e) {
          e.printStackTrace();
          throw new RuntimeException(e);
        }
      }

      LOG.info("End of handshake: {}", System.currentTimeMillis() - startTime);

      final DirectByteArrayOutputStream bos = new DirectByteArrayOutputStream();
      try {
        bos.write(sideInputDecodedFactory);
        final EncoderFactory.Encoder encoder = encoderFactory.create(bos);
        encoder.encode(output);
        bos.close();
      } catch (final IOException e) {
        throw new RuntimeException(e);
      }

      final byte[] sideInputBytes = bos.getBufDirectly();

      for (int index = 0; index < channels.size(); index++) {
        final Channel channel = channels.get(index);
        final int ind = index;
        executorService.submit(() -> {
          // 2. send side input
          //LOG.info("Write side input: {}", sideInputBytes);
          LOG.info("Write side input to {}", channel);
          channel.writeAndFlush(new NemoEvent(NemoEvent.Type.SIDE, sideInputBytes, bos.getCount()));

          // 3. send main inputs
          final MemoryStorageObject obj = list.get(ind);
          obj.close();
          channel.writeAndFlush(new NemoEvent(NemoEvent.Type.MAIN,
            obj.outputStream.getBufDirectly(), obj.outputStream.getCount()));
          LOG.info("Write {} main input to {}", obj.cnt, channel);
        });
      }

      LOG.info("{}: # of invocations: {}, # of message send: {}",
        System.currentTimeMillis() - startTime, list.size(), channels.size());

      // wait results
      final List<Object> results = futures.stream().map(future -> {
        try {
          return future.get().getPayload().toString();
        } catch (InterruptedException e) {
          e.printStackTrace();
          throw new RuntimeException(e);
        } catch (ExecutionException e) {
          e.printStackTrace();
          throw new RuntimeException(e);
        }
      }).collect(Collectors.toList());

      prefixAndObjectMap.remove(sideInputKey);
      prefixAndSizeMap.remove(sideInputKey);

      return results;
    }
  }

}



