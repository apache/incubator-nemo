package org.apache.nemo.runtime.executor.offloading;

import io.netty.channel.Channel;
import io.netty.channel.group.ChannelGroup;
import io.netty.channel.group.DefaultChannelGroup;
import io.netty.util.concurrent.GlobalEventExecutor;
import org.apache.nemo.common.EventHandler;
import org.apache.nemo.common.NemoEvent;
import org.apache.nemo.common.coder.DecoderFactory;
import org.apache.nemo.common.coder.EncoderFactory;
import org.apache.nemo.runtime.common.offloading.NemoEventHandler;
import org.apache.nemo.runtime.common.offloading.NettyServerSideChannelHandler;
import org.apache.nemo.runtime.common.offloading.NettyServerTransport;
import org.apache.nemo.runtime.executor.offloading.lambda.LambdaOffloadingRequester;
import org.apache.reef.wake.remote.ports.TcpPortProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.util.List;
import java.util.Map;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;

public final class OffloadingWorkerManager {
//
//  private static final Logger LOG = LoggerFactory.getLogger(OffloadingWorkerManager.class.getName());
//
//  private final ChannelGroup serverChannelGroup = new DefaultChannelGroup(GlobalEventExecutor.INSTANCE);
//  private NemoEventHandler nemoEventHandler;
//  private Map<Channel, EventHandler> channelEventHandlerMap;
//
//  private final NettyServerTransport nettyServerTransport;
//  private final ExecutorService executorService = Executors.newCachedThreadPool();
//
//  private final AtomicBoolean initialized = new AtomicBoolean(false);
//
//  private final OffloadingRequester offloadingRequester;
//
//  @Inject
//  private OffloadingWorkerManager(final TcpPortProvider tcpPortProvider,) {
//    this.channelEventHandlerMap = new ConcurrentHashMap<>();
//    this.nemoEventHandler = new NemoEventHandler(channelEventHandlerMap);
//    this.nettyServerTransport = new NettyServerTransport(
//      tcpPortProvider, new NettyServerSideChannelHandler(serverChannelGroup, nemoEventHandler));
//
//    LOG.info("Netty server lambda transport created end");
//    initialized.set(true);
//    this.offloadingRequester = new LambdaOffloadingRequester(
//      nemoEventHandler, nettyServerTransport.getPublicAddress(), nettyServerTransport.getPort());
//    //this.offloadingRequester = new VMOffloadingRequester(
//    //  nemoEventHandler, PUBLIC_ADDRESS, PORT);
//    offloadingRequester.start();
//  }
//
//  public Object takeResult() {
//    try {
//      return nemoEventHandler.getResultQueue().take();
//    } catch (InterruptedException e) {
//      e.printStackTrace();
//      throw new RuntimeException(e);
//    }
//  }
//
//  public void destroy(){
//    offloadingRequester.destroy();
//  }
//
//  public void setChannelHandler(final Channel channel, final EventHandler eventHandler) {
//    channelEventHandlerMap.put(channel, eventHandler);
//  }
//
//  public Future<Channel> createLambdaWorker(final List<String> serializedVertices,
//                                            final DecoderFactory inputDecoderFactory,
//                                            final EncoderFactory outputEncoderFactory) {
//
//    /*
//    final StringBuilder sb = new StringBuilder("");
//    for (final String serializedVertex : serializedVertices) {
//      sb.append("\"");
//      sb.append(serializedVertex);
//      sb.append("\"");
//      if (serializedVertices.indexOf(serializedVertex) + 1 < serializedVertices.size()) {
//        sb.append(",");
//      }
//      sb.append("]");
//    }
//    */
//
//    //System.out.println("Serialized vertices: " + serializedVertices.size());
//
//    ByteArrayOutputStream bos = new ByteArrayOutputStream();
//    ObjectOutputStream oos = null;
//    try {
//      oos = new ObjectOutputStream(bos);
//      oos.writeObject(serializedVertices);
//      oos.close();
//      bos.close();
//    } catch (IOException e) {
//      e.printStackTrace();
//      throw new RuntimeException(e);
//    }
//
//    byte[] serializedVerticesBytes = bos.toByteArray();
//    //System.out.println("Serialized vertices size: " + serializedVerticesBytes.length);
//
//    executorService.execute(() -> {
//      // Trigger lambdas
//      if (nemoEventHandler.getPendingRequest().getAndDecrement() <= 0) {
//        // add 2 for the decrement and for the new channel request
//        nemoEventHandler.getPendingRequest().addAndGet(1);
//        offloadingRequester.createChannelRequest();
//      }
//    });
//
//    return new Future<Channel>() {
//      @Override
//      public boolean cancel(boolean mayInterruptIfRunning) {
//        return false;
//      }
//
//      @Override
//      public boolean isCancelled() {
//        return false;
//      }
//
//      @Override
//      public boolean isDone() {
//        return false;
//      }
//
//      @Override
//      public Channel get() throws InterruptedException, ExecutionException {
//        try {
//          final Channel channel = nemoEventHandler.getReadyQueue().take().left();
//          channel.writeAndFlush(new NemoEvent(NemoEvent.Type.VERTICES,
//            serializedVerticesBytes, serializedVerticesBytes.length));
//          return channel;
//        } catch (InterruptedException e) {
//          e.printStackTrace();
//          throw new RuntimeException(e);
//        }
//      }
//
//      @Override
//      public Channel get(long timeout, TimeUnit unit) throws InterruptedException, ExecutionException, TimeoutException {
//        return get();
//      }
//    };
//  }
//

}
