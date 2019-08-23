/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.nemo.runtime.lambda;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import io.netty.bootstrap.Bootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.util.concurrent.DefaultThreadFactory;
//import org.apache.beam.vendor.guava.v20_0.com.google.common.collect.ImmutableBiMap;
//import org.apache.beam.vendor.guava.v20_0.com.google.common.collect.ImmutableMap;
import org.apache.beam.vendor.guava.v20_0.com.google.common.collect.ImmutableBiMap;
import org.apache.log4j.Level;
import org.apache.nemo.runtime.common.plan.Task;
import org.apache.nemo.runtime.lambda.common.Executor;
import org.apache.nemo.runtime.master.resource.LambdaEvent;
import org.apache.nemo.runtime.master.resource.NettyChannelInitializer;

import java.io.*;
import java.net.InetSocketAddress;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.Map;
import java.util.concurrent.CountDownLatch;

/**
 * LambdaExecutor deployed on AWS Lambda.
 */
public final class LambdaExecutor implements RequestHandler<Map<String, Object>, Context> {
  private Channel openChannel;
  private Bootstrap clientBootstrap;
  private EventLoopGroup clientWorkerGroup;
  private final LambdaExecutorInboundHandler lambdaExecutorInboundHandler = new LambdaExecutorInboundHandler();
  private LambdaEventHandler lambdaEventHandler;

  private String serverAddr;
  private int serverPort;

  private static transient CountDownLatch workerComplete;

  /**
   * Reads address and port from the netty server.
   * @param input
   * @param context
   * @return
   */
  @Override
  public Context handleRequest(final Map<String, Object> input, final Context context) {
    this.workerComplete = new CountDownLatch(1);
    this.lambdaEventHandler =  new LambdaEventHandler(this.workerComplete);

    org.apache.log4j.Logger.getRootLogger().setLevel(Level.OFF);

    final String address = (String) input.get("address");
    final int port = (Integer) input.get("port");
    final String requestedAddr = "/" + address + ":" + port;
    System.out.println("Requested addr: " + requestedAddr);
    this.serverAddr = address;
    this.serverPort = port;

    // open channel
    this.clientWorkerGroup = new NioEventLoopGroup(1,
      new DefaultThreadFactory("hello" + "-ClientWorker"));
    this.clientBootstrap = new Bootstrap();
    this.clientBootstrap.group(clientWorkerGroup)
      .channel(NioSocketChannel.class)
      .handler(new NettyChannelInitializer(this.lambdaExecutorInboundHandler))
      .option(ChannelOption.SO_REUSEADDR, true)
      .option(ChannelOption.SO_KEEPALIVE, true);

    /**
     * TODO #407: LambdaHandler for single-stage execution
     * Currently LambdaExecutor only sets up connection with Nemo LambdaMaster.
     * LambdaExecutor is expected to receive tasks from the opened channel, process the tasks,
     * and send the processed results back to LambdaMaster.
     */
    this.openChannel = channelOpen(input);
    this.lambdaExecutorInboundHandler.setEventHandler(this.lambdaEventHandler);

    /**
     * Register open channel to LambdaEventHandler, so that
     * LambdaEvent from Nemo will be processed and results will be returned.
     */
    while (this.workerComplete.getCount() > 0) {
      try {
        Thread.sleep(1000);
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
    }
    return null;
  }

  public void onTaskReceived(final Task task) {
    Executor e = new Executor();
    e.onTaskReceived(task);
  }

  /**
   * Parse input.
   * @param input
   * @return
   */
  private Channel channelOpen(final Map<String, Object> input) {
    // Connect to the LambdaMaster
    final ChannelFuture channelFuture;
    channelFuture = this.clientBootstrap.connect(new InetSocketAddress(this.serverAddr, this.serverPort));
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


  public final class LambdaEventHandler {

    private Channel channel;
    private transient CountDownLatch workerComplete;

    public LambdaEventHandler(final CountDownLatch workerComplete) {
      this.workerComplete = workerComplete;
    }

    public void setChannel(Channel channel) {
      this.channel = channel;
    }

    public synchronized void onNext(final LambdaEvent nemoEvent) {
      System.out.println("LambdaEventHandler->onNext " + nemoEvent.getType());
      switch (nemoEvent.getType()) {
        case WORKER_INIT:
          Task task;
          try {
            // Task passed in a byte array
            byte[] bytes = nemoEvent.getBytes();
            ByteArrayInputStream byteInputStream = new ByteArrayInputStream(bytes);
//           ObjectInputStream objectInputStream = new ObjectInputStream(byteInputStream);
            //task = SerializationUtils.deserialize(nemoEvent.getBytes());
//           task = (Task) objectInputStream.readObject();
            URL[] urls = null;
            try {
              // Convert the file object to a URL
              File dir = new File(System.getProperty("/opt/java/lib/beam-vendor-guava-20_0-0.1.jar")
                + File.separator + "dir" + File.separator);
              URL url = dir.toURL();
              urls = new URL[]{url};
              ClassLoader urlClassLoader = new URLClassLoader(urls);
              ObjectInputStream ois = new ObjectInputStream(byteInputStream) {
                @SuppressWarnings("rawtypes")
                @Override
                protected Class resolveClass(ObjectStreamClass objectStreamClass)
                  throws IOException, ClassNotFoundException {
                  if (objectStreamClass.getName().equals(ImmutableBiMap.class.getName())) {
                    System.out.println("Use urlClassLoader");
                    return Class.forName(objectStreamClass.getName(), true, urlClassLoader);
                  } else {
                    System.out.println("Don't use urlClassLoader");
                    return Class.forName(objectStreamClass.getName(), true, ClassLoader.getSystemClassLoader());
                  }
                }
              };
              task = (Task) ois.readObject();

              System.out.println("Decode task successfully" + task.toString());
              onTaskReceived(task);
            } catch (Exception e) {
              e.printStackTrace();
              System.out.println("Read LambdaEvent bytebuf error");
            }
          } catch (Exception e) {
            e.printStackTrace();
          }
          break;
        case DATA:
          throw new UnsupportedOperationException("DATA not supported");
        case END:
          // end of event
          System.out.println("END received");
          this.workerComplete.countDown();
          break;
        case WARMUP_END:
          throw new UnsupportedOperationException("WARMUP_END not supported");
      }
    }
  }

}
