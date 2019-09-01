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
package org.apache.nemo.runtime.executor.bytetransfer;

import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFutureListener;
import org.apache.nemo.common.TaskLoc;
import org.apache.nemo.offloading.common.EventHandler;
import org.apache.nemo.runtime.executor.common.datatransfer.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.io.InputStream;
import java.util.Iterator;
import java.util.Queue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * Container for multiple input streams. Represents a transfer context on receiver-side.
 *
 * <h3>Thread safety:</h3>
 * <p>Methods with default access modifier, namely {@link #onNewStream()}, {@link #onByteBuf(ByteBuf)},
 * {@link #onContextClose()}, are not thread-safe, since they are called by a single Netty event loop.</p>
 * <p>Public methods are thread safe,
 * although the execution order may not be linearized if they were called from different threads.</p>
 */
public final class LocalByteInputContext implements ByteInputContext{
  @Override
  public Iterator<InputStream> getInputStreams() {
    return null;
  }

  @Override
  public CompletableFuture<Iterator<InputStream>> getCompletedFuture() {
    return null;
  }

  @Override
  public void onNewStream() {

  }

  @Override
  public void setTaskId(String taskId) {

  }

  @Override
  public void sendStopMessage(EventHandler<Integer> pendingAckHandler) {

  }

  @Override
  public void receivePendingAck() {

  }

  @Override
  public void receiveStopSignalFromParent(ByteTransferContextSetupMessage msg, TaskLoc sendDataTo) {

  }

  @Override
  public void receiveStopSignalFromParent(TaskLoc sendDataTo) {

  }

  @Override
  public void receiveRestartSignalFromParent(Channel channel, ByteTransferContextSetupMessage msg) {

  }

  @Override
  public void restart(String taskId) {

  }

  @Override
  public void setupRestartChannel(Channel channel, ByteTransferContextSetupMessage msg) {

  }

  @Override
  public void onByteBuf(ByteBuf byteBuf) {

  }

  @Override
  public boolean isFinished() {
    return false;
  }

  @Override
  public void onContextClose() {

  }

  @Override
  public void onContextStop() {

  }

  @Override
  public void onContextRestart() {

  }

  @Override
  public String getRemoteExecutorId() {
    return null;
  }

  @Override
  public ContextId getContextId() {
    return null;
  }

  @Override
  public byte[] getContextDescriptor() {
    return new byte[0];
  }

  @Override
  public boolean hasException() {
    return false;
  }

  @Override
  public Throwable getException() {
    return null;
  }

  @Override
  public ChannelFutureListener getChannelWriteListener() {
    return null;
  }

  @Override
  public void onChannelError(@Nullable Throwable cause) {

  }

  @Override
  public void setChannelError(@Nullable Throwable cause) {

  }

  @Override
  public void deregister() {

  }

//  private static final Logger LOG = LoggerFactory.getLogger(LocalByteInputContext.class.getName());
//  private final Queue<Object> objectQueue;
//  private final IteratorWithNumBytes iteratorWithNumBytes;
//  private final LocalByteOutputContext localByteOutputContext;
//
//  private  boolean isFinished = false;
//  private EventHandler<Integer> ackHandler;
//  private final ScheduledExecutorService ackService;
//
//  /**
//   * Creates an input context.
//   * @param remoteExecutorId    id of the remote executor
//   * @param contextId           identifier for this context
//   * @param contextDescriptor   user-provided context descriptor
//   * @param contextManager      {@link ContextManager} for the channel
//   */
//  LocalByteInputContext(final String remoteExecutorId,
//                        final ContextId contextId,
//                        final byte[] contextDescriptor,
//                        final ContextManager contextManager,
//                        final Queue<Object> objectQueue,
//                        final LocalByteOutputContext localByteOutputContext,
//                        final ScheduledExecutorService ackService) {
//    super(remoteExecutorId, contextId, contextDescriptor, contextManager);
//    this.objectQueue = objectQueue;
//    this.iteratorWithNumBytes = new QueueIteratorWithNumBytes();
//    this.localByteOutputContext = localByteOutputContext;
//    this.ackService = ackService;
//  }
//
//  public LocalByteOutputContext getLocalByteOutputContext() {
//    return localByteOutputContext;
//  }
//
//  /**
//   * Returns {@link Iterator} of {@link InputStream}s.
//   * This method always returns the same {@link Iterator} instance.
//   * @return {@link Iterator} of {@link InputStream}s.
//   */
//  public Iterator<InputStream> getInputStreams() {
//    throw new UnsupportedOperationException();
//  }
//
//  @Override
//  public void receiveFromSF(Channel channel) {
//    throw new UnsupportedOperationException();
//  }
//
//  @Override
//  public void receiveFromVM(Channel channel) {
//    throw new UnsupportedOperationException();
//  }
//
//  public IteratorWithNumBytes getIteratorWithNumBytes() {
//    return iteratorWithNumBytes;
//  }
//
//  public Queue<Object> getQueue() {
//    return objectQueue;
//  }
//
//  /**
//   * Returns a future, which is completed when the corresponding transfer for this context gets done.
//   * @return a {@link CompletableFuture} for the same value that {@link #getInputStreams()} returns
//   */
//  public CompletableFuture<Iterator<InputStream>> getCompletedFuture() {
//    throw new UnsupportedOperationException();
//  }
//
//  /**
//   * Called when a punctuation for sub-stream incarnation is detected.
//   */
//  @Override
//  public void onNewStream() {
//    throw new UnsupportedOperationException();
//  }
//
//  @Override
//  public void sendMessage(ByteTransferContextSetupMessage message,
//                          final EventHandler<Integer> handler) {
//    ackHandler = handler;
//
//    LOG.info("Send message: {} at {}", message.getMessageType(),
//      getContextId().getTransferIndex());
//
//
//    throw new UnsupportedOperationException("Not supported");
//
//    /*
//    switch (message.getMessageType()) {
//      case SIGNAL_FROM_CHILD_FOR_STOP_OUTPUT: {
//        localByteOutputContext.pending(SF, "", 1);
//        break;
//      }
//      case RESUME_AFTER_SCALEOUT_VM: {
//        throw new RuntimeException("Unsupported");
//        //localByteOutputContext.scaleoutToVm(message.getMovedAddress(), message.getTaskId());
//      }
//      case STOP_OUTPUT_FOR_SCALEIN: {
//        localByteOutputContext.pending(VM, "", 1);
//        break;
//      }
//      case RESUME_AFTER_SCALEIN_DOWNSTREAM_VM: {
//        //localByteOutputContext.scaleInToVm();
//        break;
//      }
//      default: {
//        throw new UnsupportedOperationException("Not supported type: " + message.getMessageType());
//      }
//    }
//    */
//  }
//
//  @Override
//  public void receivePendingAck() {
//    if (objectQueue.isEmpty()) {
//      ackHandler.onNext(1);
//    } else {
//      // check ack
//      ackService.schedule(new AckRunner(), 500, TimeUnit.MILLISECONDS);
//    }
//  }
//
//  /**
//   * Called when {@link ByteBuf} is supplied to this context.
//   * @param byteBuf the {@link ByteBuf} to supply
//   */
//  @Override
//  public void onByteBuf(final ByteBuf byteBuf) {
//    throw new UnsupportedOperationException();
//  }
//
//  @Override
//  public boolean isFinished() {
//    return isFinished;
//  }
//
//  /**
//   * Called when {@link #onByteBuf(ByteBuf)} event is no longer expected.
//   */
//  @Override
//  public void onContextClose() {
//    isFinished = true;
//  }
//
//  @Override
//  public void onContextStop() {
//    LOG.info("Local context finished true {}", getContextId());
//    isFinished = true;
//  }
//
//  @Override
//  public void onContextRestart() {
//    isFinished = false;
//  }
//
//  @Override
//  public void onChannelError(@Nullable final Throwable cause) {
//    setChannelError(cause);
//    throw new RuntimeException(cause);
//  }
//
//  final class QueueIteratorWithNumBytes implements IteratorWithNumBytes<Object> {
//
//    private Object nextData;
//
//    @Override
//    public boolean isFinished() {
//      return isFinished;
//    }
//
//    @Override
//    public long getNumSerializedBytes() throws NumBytesNotSupportedException {
//      return 0;
//    }
//
//    @Override
//    public long getNumEncodedBytes() throws NumBytesNotSupportedException {
//      return 0;
//    }
//
//    @Override
//    public boolean hasNext() {
//      if (objectQueue.isEmpty() || isFinished) {
//        return false;
//      }
//
//      return true;
//      /*
//      while (!isFinished) {
//        if (!objectQueue.isEmpty()) {
//          final Object data = objectQueue.poll();
//          nextData = data;
//          return true;
//        } else {
//          try {
//            Thread.sleep(200);
//          } catch (InterruptedException e) {
//            e.printStackTrace();
//          }
//        }
//      }
//      return false;
//      */
//    }
//
//    @Override
//    public Object next() {
//      return objectQueue.poll();
//    }
//  }
//
//
//  final class AckRunner implements Runnable {
//
//    @Override
//    public void run() {
//      if (objectQueue.isEmpty()) {
//        ackHandler.onNext(1);
//      } else {
//        ackService.schedule(new AckRunner(), 500, TimeUnit.MILLISECONDS);
//      }
//    }
//  }
}
