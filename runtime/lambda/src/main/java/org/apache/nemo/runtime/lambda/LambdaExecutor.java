/*
 * Licensed to the Apache Software Foundation (ASF) under one
 or more contributor license agreements.  See the NOTICE file
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
import com.google.common.collect.ImmutableSet;
import io.netty.bootstrap.Bootstrap;
import io.netty.channel.*;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.util.concurrent.DefaultThreadFactory;
import org.apache.log4j.Level;
import org.apache.nemo.runtime.common.plan.Task;
import org.apache.nemo.runtime.lambda.common.Executor;
import org.apache.nemo.runtime.master.resource.LambdaEvent;
import org.apache.nemo.runtime.master.resource.NettyChannelInitializer;

import java.io.*;
import java.net.InetSocketAddress;
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
    LambdaEvent endEvent = new LambdaEvent(LambdaEvent.Type.END);

    try {
      if (openChannel.isActive()) {
        this.openChannel.writeAndFlush(endEvent);
      } else {
        System.out.println("Channel not active, cannot write to LambdaMaster");
      }
    } catch (Exception e) {
      System.out.println("Flushing END LambdaEvent error");
      e.printStackTrace();
    } finally {
      this.clientWorkerGroup.shutdownGracefully().awaitUninterruptibly();
    }
    return null;
  }

  public void onTaskReceived(final Task task) {
    Executor e = new Executor();
    e.onTaskReceived(task);
    System.out.println("Task executed");
    this.workerComplete.countDown();
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

    private transient CountDownLatch workerComplete;

    public LambdaEventHandler(final CountDownLatch workerComplete) {
      this.workerComplete = workerComplete;
    }

    public void onNext(final LambdaEvent nemoEvent) {
      System.out.println("LambdaEventHandler->onNext " + nemoEvent.getType());
      switch (nemoEvent.getType()) {
        case WORKER_INIT:
          Task task = null;
          try {
            // Task passed in a byte array
            byte[] bytes = nemoEvent.getBytes();
            ByteArrayInputStream byteInputStream = new ByteArrayInputStream(bytes);

            try {
              final ImmutableSet.Builder<Object> notFoundClasses = ImmutableSet.builder();
              try {
                ObjectInputStream objectInputStream = new ObjectInputStream(byteInputStream) {
                  @Override
                  protected Class<?> resolveClass(ObjectStreamClass desc) throws IOException, ClassNotFoundException {
                    try {
                      return super.resolveClass(desc);
                    } catch (ClassNotFoundException e) {
                      notFoundClasses.add(desc.getName());
                      throw e;
                    }
                  }
                };
                task = (Task) objectInputStream.readObject();
              } catch (ClassCastException e) {
                e.printStackTrace();
                System.out.println("\"ClassCastException while de-serializing , classes not found are: " + notFoundClasses.build());
              } catch (IOException | ClassNotFoundException e) {
                e.printStackTrace();
                System.out.println("Could not deserialize ");
              }
             if(task != null) {
                System.out.println("Decode task successfully" + task.toString());
                onTaskReceived(task);
              }
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
