package org.apache.nemo.offloading.workers.vm;


public final class VMOffloadingRequester {
//
//  private static final Logger LOG = LoggerFactory.getLogger(VMOffloadingRequester.class.getName());
//
//  private final ScheduledExecutorService warmer = Executors.newSingleThreadScheduledExecutor();
//  private final int warmupPeriod = 90; // sec
//  private final AWSLambdaAsync awsLambda;
//  private final OffloadingEventHandler nemoEventHandler;
//
//  private final ExecutorService executorService = Executors.newFixedThreadPool(30);
//  private final AmazonEC2 ec2 = AmazonEC2ClientBuilder.defaultClient();
//
//  private final ChannelGroup serverChannelGroup = new DefaultChannelGroup(GlobalEventExecutor.INSTANCE);
//  private EventLoopGroup serverBossGroup;
//  private EventLoopGroup serverWorkerGroup;
//  private Channel acceptor;
//  private Map<Channel, org.apache.nemo.offloading.common.EventHandler> channelEventHandlerMap;
//
//  private final List<String> vmAddresses = Arrays.asList("172.31.37.143");
//  private final List<String> instanceIds = Arrays.asList("i-0148d7ea6eae4cc80");
//
//  private final String serverAddress;
//  private final int serverPort;
//
//  private final List<Channel> readyVMs = new LinkedList<>();
//
//  private EventLoopGroup clientWorkerGroup;
//
//  private final ConcurrentMap<Channel, EventHandler<OffloadingEvent>> map;
//
//  private final AtomicBoolean stopped = new AtomicBoolean(true);
//
//  int channelIndex = 0;
//
//
//  /**
//   * Netty client bootstrap.
//   */
//  private Bootstrap clientBootstrap;
//
//  public VMOffloadingRequester(final OffloadingEventHandler nemoEventHandler,
//                               final String serverAddress,
//                               final int port) {
//    this.awsLambda = AWSLambdaAsyncClientBuilder.standard().withClientConfiguration(
//      new ClientConfiguration().withMaxConnections(500)).build();
//    this.nemoEventHandler = nemoEventHandler;
//    this.serverAddress = serverAddress;
//    this.serverPort = port;
//
//    this.clientWorkerGroup = new NioEventLoopGroup(1,
//      new DefaultThreadFactory("hello" + "-ClientWorker"));
//    this.clientBootstrap = new Bootstrap();
//    this.map = new ConcurrentHashMap<>();
//    this.clientBootstrap.group(clientWorkerGroup)
//      .channel(NioSocketChannel.class)
//      .handler(new NettyChannelInitializer(new NettyLambdaInboundHandler(map)))
//      .option(ChannelOption.SO_REUSEADDR, true)
//      .option(ChannelOption.SO_KEEPALIVE, true);
//  }
//
//  @Override
//  public void start() {
//    // ping pong
//
//  }
//
//  @Override
//  public void createChannelRequest() {
//    final long waitingTime = 2000;
//
//    executorService.execute(() -> {
//      if (stopped.compareAndSet(true, false)) {
//        // 1 start instance
//        DescribeInstancesRequest request = new DescribeInstancesRequest();
//        request.setInstanceIds(instanceIds);
//        DescribeInstancesResult response = ec2.describeInstances(request);
//
//        for(final Reservation reservation : response.getReservations()) {
//          for(final Instance instance : reservation.getInstances()) {
//            while (true) {
//              if (instance.getState().getName().equals("stopped")) {
//                // ready to start
//                final StartInstancesRequest startRequest = new StartInstancesRequest()
//                  .withInstanceIds(instanceIds);
//                LOG.info("Starting ec2 instances {}/{}", instanceIds, System.currentTimeMillis());
//                ec2.startInstances(startRequest);
//                LOG.info("End of Starting ec2 instances {}/{}", instanceIds, System.currentTimeMillis());
//                break;
//              } else if (instance.getState().getName().equals("stopping")) {
//                // waiting...
//                try {
//                  Thread.sleep(2000);
//                } catch (InterruptedException e) {
//                  e.printStackTrace();
//                }
//              } else {
//                throw new RuntimeException("Unsupported state type: " + instance.getState().getName());
//              }
//            }
//          }
//        }
//
//        // 2 connect to the instance
//        for (final String address : vmAddresses) {
//          ChannelFuture channelFuture;
//          while (true) {
//            final long st = System.currentTimeMillis();
//            channelFuture = clientBootstrap.connect(new InetSocketAddress(address, VM_WORKER_PORT));
//            channelFuture.awaitUninterruptibly(waitingTime);
//            assert channelFuture.isDone();
//            if (!channelFuture.isSuccess()) {
//              LOG.warn("A connection failed for " + address + "  waiting...");
//              final long elapsedTime = System.currentTimeMillis() - st;
//              try {
//                Thread.sleep(Math.max(1, waitingTime - elapsedTime));
//              } catch (InterruptedException e) {
//                e.printStackTrace();
//              }
//            } else {
//              break;
//            }
//          }
//
//          final Channel openChannel = channelFuture.channel();
//          LOG.info("Open channel for VM: {}", openChannel);
//          synchronized (readyVMs) {
//            readyVMs.add(openChannel);
//          }
//          LOG.info("Add channel: {}", openChannel);
//        }
//      }
//
//      Channel requestChannel;
//
//      while (true) {
//        synchronized (readyVMs) {
//          LOG.info("ReadyVM: {} ", readyVMs);
//          if (readyVMs.size() > 0) {
//            final int idx = (channelIndex + 1) % readyVMs.size();
//            requestChannel = readyVMs.get(idx);
//            break;
//          }
//        }
//
//        try {
//          Thread.sleep(1000);
//        } catch (InterruptedException e) {
//          e.printStackTrace();
//        }
//      }
//
//      LOG.info("Request to Channel {}", requestChannel);
//      final byte[] bytes = String.format("{\"address\":\"%s\", \"port\": %d}",
//        serverAddress, serverPort).getBytes();
//      requestChannel.writeAndFlush(new OffloadingEvent(OffloadingEvent.Type.CLIENT_HANDSHAKE, bytes, bytes.length));
//
//    });
//  }
//
//  @Override
//  public void destroy() {
//    synchronized (readyVMs) {
//      readyVMs.clear();
//    }
//    LOG.info("Stopping instances {}", instanceIds);
//    final StopInstancesRequest request = new StopInstancesRequest()
//      .withInstanceIds(instanceIds);
//    ec2.stopInstances(request);
//    stopped.set(true);
//  }
//
//  private void startAndStop() {
//
//  }
//
//  private void createAndDestroy() {
//
//  }
//
//  @Override
//  public void close() {
//
//  }
}
