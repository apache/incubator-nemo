package org.apache.nemo.runtime.master.offloading;


import io.netty.bootstrap.Bootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.util.concurrent.DefaultThreadFactory;
import org.apache.nemo.conf.EvalConf;
import org.apache.nemo.offloading.common.EventHandler;
import org.apache.nemo.offloading.common.NettyChannelInitializer;
import org.apache.nemo.offloading.common.NettyLambdaInboundHandler;
import org.apache.nemo.offloading.common.OffloadingMasterEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.InetSocketAddress;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.apache.nemo.offloading.common.Constants.VM_WORKER_PORT;

public final class LocalExecutorOffloadingRequester implements OffloadingRequester {

  private static final Logger LOG = LoggerFactory.getLogger(LocalExecutorOffloadingRequester.class.getName());

  //private final List<Channel> readyVMs = new LinkedList<>();

  private EventLoopGroup clientWorkerGroup;

  private final ConcurrentMap<Channel, EventHandler<OffloadingMasterEvent>> map;

  private final AtomicBoolean stopped = new AtomicBoolean(true);

  private final AtomicInteger requestId = new AtomicInteger(0);
  /**
   * Netty client bootstrap.
   */
  private Bootstrap clientBootstrap;

  // key: remoteAddress, value: instanceId
  private final Map<String, String> vmChannelMap = new ConcurrentHashMap<>();

  private final AtomicInteger numVMs = new AtomicInteger(0);
  private final ExecutorService waitingExecutor = Executors.newCachedThreadPool();
  private final int cpulimit;

  @Inject
  private LocalExecutorOffloadingRequester(final EvalConf evalConf) {
    this.clientWorkerGroup = new NioEventLoopGroup(10,
      new DefaultThreadFactory("hello" + "-ClientWorker"));
    this.clientBootstrap = new Bootstrap();
    this.cpulimit = (int) (evalConf.cpuLimit * 100);
    this.map = new ConcurrentHashMap<>();
    this.clientBootstrap.group(clientWorkerGroup)
      .channel(NioSocketChannel.class)
      .handler(new NettyChannelInitializer(new NettyLambdaInboundHandler(map)))
      .option(ChannelOption.SO_REUSEADDR, true)
      .option(ChannelOption.SO_KEEPALIVE, true);
  }

  @Override
  public void start() {
    // ping pong

  }

  @Override
  public synchronized void destroyChannel(final Channel channel) {
    final String addr = channel.remoteAddress().toString().split(":")[0];
    final String instanceId = vmChannelMap.remove(addr);
    numVMs.getAndDecrement();
    LOG.info("Stopping instance {}, channel: {}", instanceId, addr);
  }

  private final int port = new Random(System.currentTimeMillis()).nextInt(500)
   + VM_WORKER_PORT;

  private final AtomicInteger atomicInteger = new AtomicInteger(0);

  @Override
  public synchronized void createChannelRequest(String serverAddr, int serverPort,
                                                int requestId) {
    final int myPort = port + atomicInteger.getAndIncrement();
    // final String nemo_home = System.getenv("NEMO_HOME");
    final String nemo_home = "/home/taegeonum/incubator-nemo";
    LOG.info("Creating VM worker with port " + myPort);
    final String path = nemo_home + "/offloading/workers/vm/target/offloading-vm-0.2-SNAPSHOT-shaded.jar";
      waitingExecutor.execute(() -> {
        try {

          //LOG.info("cpulimit -l " + cpulimit + " java -cp " + path + " org.apache.nemo.offloading.workers.vm.VMWorker " + myPort + " " + 10000000);
          LOG.info("java -cp " + path + " org.apache.nemo.offloading.workers.vm.VMWorker " + myPort + " " + 10000000);
          Process p = Runtime.getRuntime().exec(
               "java -cp " + path + " org.apache.nemo.offloading.workers.vm.VMWorker " + myPort + " " + 10000000);
         //   "cpulimit -l " + cpulimit + " java -cp " + path + " org.apache.nemo.offloading.workers.vm.VMWorker " + myPort + " " + 10000000);

          String line;
          BufferedReader in = new BufferedReader(
            new InputStreamReader(p.getInputStream()) );

          BufferedReader stdError = new BufferedReader(new
            InputStreamReader(p.getErrorStream()));


          while (true) {
            while (in.ready() && (line = in.readLine()) != null) {
              LOG.info("[VMWworker " + myPort + "]: " + line);
            }
            // in.close();
            // LOG.info("End of read line !!!!!!!!!!!!!!!!!!!!");

            while (stdError.ready() && (line = stdError.readLine()) != null) {
              LOG.info("[VMWworker " + myPort + "]: " + line);
            }
            // stdError.close();

            try {
              Thread.sleep(300);
            } catch (InterruptedException e) {
              e.printStackTrace();
            }
          }

        } catch (IOException e) {
          e.printStackTrace();
          throw new RuntimeException(e);
        }
      });
      waitInstance(myPort, serverAddr, serverPort);
    LOG.info("Create request at VMOffloadingREquestor");
  }


  private void waitInstance(final int myPort,
                            final String serverAddr,
                            final int serverPort) {
    final long waitingTime = 1000;

    waitingExecutor.execute(() -> {
      ChannelFuture channelFuture;
      while (true) {
        final long st = System.currentTimeMillis();
        channelFuture = clientBootstrap.connect(new InetSocketAddress("localhost", myPort));
        channelFuture.awaitUninterruptibly(waitingTime);
        assert channelFuture.isDone();
        if (!channelFuture.isSuccess()) {
          LOG.warn("A connection failed for localhost  waiting...");
          final long elapsedTime = System.currentTimeMillis() - st;
          try {
            Thread.sleep(waitingTime);
          } catch (InterruptedException e) {
            e.printStackTrace();
          }
        } else {
          break;
        }
      }

      final Channel openChannel = channelFuture.channel();
      LOG.info("Open channel for VM: {}", openChannel);

      // send handshake
      final byte[] bytes = String.format("{\"address\":\"%s\", \"port\": %d, \"requestId\": %d}",
        serverAddr, serverPort, requestId.getAndIncrement()).getBytes();
      openChannel.writeAndFlush(new OffloadingMasterEvent(OffloadingMasterEvent.Type.SEND_ADDRESS, bytes, bytes.length));

      LOG.info("Add channel: {}, address: {}", openChannel, openChannel.remoteAddress());

      //return openChannel;
    });
  }


  @Override
  public void destroy() {
    /*
    synchronized (readyVMs) {
      readyVMs.clear();
    }
    */
    stopped.set(true);
  }

  @Override
  public void close() {

  }
}
