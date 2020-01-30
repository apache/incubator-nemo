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

import io.netty.channel.Channel;
import io.netty.channel.ChannelFutureListener;
import org.apache.nemo.common.TaskLoc;
import org.apache.nemo.offloading.common.EventHandler;
import org.apache.nemo.runtime.executor.common.ExecutorThread;
import org.apache.nemo.runtime.executor.common.datatransfer.*;

import javax.annotation.Nullable;
import java.io.IOException;


/**
 * Container for multiple output streams. Represents a transfer context on sender-side.
 *
 * <p>Public methods are thread safe,
 * although the execution order may not be linearized if they were called from different threads.</p>
 */
public final class LocalByteOutputContext implements ByteOutputContext {

  @Override
  public void setTaskId(String taskId) {

  }

  @Override
  public void receiveStopAck() {

  }

  @Override
  public void receiveStopSignalFromChild(ByteTransferContextSetupMessage msg, TaskLoc sendDataTo) {

  }

  @Override
  public void sendStopMessage(EventHandler<Integer> ackHandler) {

  }

  @Override
  public void receiveRestartAck() {

  }

  @Override
  public void restart(String taskId) {

  }

  @Override
  public void receiveRestartSignalFromChild(Channel channel, ByteTransferContextSetupMessage msg) {

  }

  @Override
  public void setupRestartChannel(Channel channel, ByteTransferContextSetupMessage msg) {

  }

  @Override
  public ByteOutputStream newOutputStream(ExecutorThread executorThread) throws IOException {
    return null;
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

  @Override
  public void close() throws Exception {

  }
//  private static final Logger LOG = LoggerFactory.getLogger(LocalByteOutputContext.class.getName());
//
//  private final Channel channel;
//
//  private volatile boolean closed = false;
//
//  private final Queue<Object> objectQueue;
//
//    private volatile boolean isPending = false;
//
//  private TaskLoc sendDataTo = VM;
//  private final VMScalingClientTransport vmScalingClientTransport;
//  private Channel vmChannel;
//  private String vmTaskId;
//
//  private ByteInputContext localByteInputContext;
//
//  private String remoteAddress;
//
//  /**
//   * Creates a output context.
//   *
//   * @param remoteExecutorId    id of the remote executor
//   * @param contextId           identifier for this context
//   * @param contextDescriptor   user-provided context descriptor
//   * @param contextManager      {@link ContextManager} for the channel
//   */
//  LocalByteOutputContext(final String remoteExecutorId,
//                         final ContextId contextId,
//                         final byte[] contextDescriptor,
//                         final ContextManager contextManager,
//                         final Queue<Object> objectQueue,
//                         final VMScalingClientTransport vmScalingClientTransport) {
//    super(remoteExecutorId, contextId, contextDescriptor, contextManager);
//    this.channel = contextManager.getChannel();
//    this.objectQueue = objectQueue;
//    this.vmScalingClientTransport = vmScalingClientTransport;
//  }
//
//  public void setLocalByteInputContext(final ByteInputContext byteInputContext) {
//    localByteInputContext = byteInputContext;
//  }
//
//  @Override
//  public void receivePendingAck() {
//      throw new RuntimeException("Not supporteD");
//  }
//
//  /**
//   * Closes existing sub-stream (if any) and create a new sub-stream.
//   * @return new {@link ByteOutputStream}
//   * @throws IOException if an exception was set or this context was closed.
//   */
//  @Override
//  public ByteOutputStream newOutputStream(ExecutorThread t) throws IOException {
//    return new LocalByteOutputStream();
//  }
//
//  @Override
//  public void pending(final TaskLoc dt, String tid) {
//    LOG.info("LocalByteOutputContext pending: {}", getContextId().getTransferIndex());
//    sendDataTo = dt;
//    isPending = true;
//  }
//
//  @Override
//  public void scaleoutToVm(String address, String taskId) {
//        LOG.info("LocalByteOutputContext scaleout to {}/{}: {}",
//          address, taskId, getContextId().getTransferIndex());
//
//    final String[] split = address.split(":");
//    final ChannelFuture channelFuture =
//      vmScalingClientTransport.connectTo(split[0], Constants.VM_WORKER_PORT);
//    remoteAddress = split[0];
//
//    if (channelFuture.isDone()) {
//      vmChannel = channelFuture.channel();
//      vmTaskId = taskId;
//      isPending = false;
//    } else {
//      channelFuture.addListener(new GenericFutureListener<Future<? super Void>>() {
//        @Override
//        public void operationComplete(Future<? super Void> future) throws Exception {
//          vmChannel = channelFuture.channel();
//          vmTaskId = taskId;
//          isPending = false;
//        }
//      });
//    }
//  }
//
//  @Override
//  public void scaleoutToVm(Channel channel) {
//      throw new RuntimeException("Not supported");
//  }
//
//  @Override
//  public void scaleoutToVmWoRestart(Channel channel) {
//    throw new RuntimeException("Not supporteD");
//  }
//
//  @Override
//  public void scaleInToVm(Channel channel) {
//    throw new RuntimeException("Not supported");
//  }
//
//  @Override
//  public void scaleInToVmWoRestart(Channel channel) {
//
//    throw new RuntimeException("Not supporteD");
//  }
//
//  public Channel getChannel() {
//    return channel;
//  }
//
//  public void stop() {
//    // just send stop message
//    LOG.info("Stop local context {}", getContextId());
//    //getContextManager().onContextStopLocal(getContextId().getTransferIndex());
//  }
//
//  public void restart(final String taskId) {
//    LOG.info("Restart local context {}", getContextId());
//    //getContextManager().onContextRestartLocal(getContextId().getTransferIndex());
//  }
//
//  @Override
//  public void sendMessage(ByteTransferContextSetupMessage msg, EventHandler<Integer> ackHandler) {
//    throw new RuntimeException("Unsupported exception");
//  }
//
//  /**
//   * Closes this stream.
//   *
//   * @throws IOException if an exception was set
//   */
//  @Override
//  public void close() throws IOException {
//    //getContextManager().onContextCloseLocal(getContextId().getTransferIndex());
//  }
//
//  @Override
//  public void onChannelError(@Nullable final Throwable cause) {
//    throw new RuntimeException(cause);
//  }
//
//  /**
//   * An {@link OutputStream} implementation which buffers data to {@link ByteBuf}s.
//   *
//   * <p>Public methods are thread safe,
//   * although the execution order may not be linearized if they were called from different threads.</p>
//   */
//  public final class LocalByteOutputStream implements ByteOutputStream {
//
//    private volatile boolean newSubStream = true;
//    private volatile boolean closed = false;
//    private final List<Object> pendingData = new ArrayList<>();
//    private final List<ByteBuf> pendingByteBufData = new ArrayList<>();
//    public Channel getChannel() {
//      return channel;
//    }
//
//    @Override
//    public void write(final int i) throws IOException {
//      throw new UnsupportedOperationException();
//    }
//
//    @Override
//    public void write(final byte[] bytes, final int offset, final int length) throws IOException {
//      throw new UnsupportedOperationException();
//    }
//
//    @Override
//    public void close() throws IOException {
//      // do nothing
//    }
//
//    private ByteBuf serializeElement(final Object element,
//                                     final Serializer serializer,
//                                     final String edgeId,
//                                     final String opId) {
//      final ByteBuf byteBuf = ByteBufAllocator.DEFAULT.buffer();
//
//      try {
//        final ByteBufOutputStream byteBufOutputStream = new ByteBufOutputStream(byteBuf);
//        byteBufOutputStream.writeBoolean(false);
//        byteBufOutputStream.writeUTF(edgeId);
//        byteBufOutputStream.writeUTF(opId);
//        final OutputStream wrapped =
//          DataUtil.buildOutputStream(byteBufOutputStream, serializer.getEncodeStreamChainers());
//        final EncoderFactory.Encoder encoder = serializer.getEncoderFactory().create(wrapped);
//        //LOG.info("Element encoder: {}", encoder);
//        encoder.encode(element);
//        wrapped.close();
//
//        // DATA ID
//        byteBufOutputStream.writeInt(0);
//
//      } catch (final Exception e) {
//        e.printStackTrace();
//        throw new RuntimeException(e);
//      }
//
//      return byteBuf;
//    }
//
//    /**
//     * Write an element to the channel.
//     * @param element element
//     * @param serializer serializer
//     */
//    @Override
//    public void writeElement(final Object element,
//                             final Serializer serializer,
//                             final String edgeId,
//                             final String opId) {
//      if (isPending) {
//        // buffer data..
//        // because currently the data processing is pending
//        switch (sendDataTo) {
//          case SF:
//            final boolean isEmpty = pendingData.isEmpty();
//
//            if (isEmpty) {
//              // ACK!!
//              localByteInputContext.receivePendingAck();
//            }
//
//            pendingByteBufData.add(serializeElement(element, serializer, edgeId, opId));
//            break;
//
//          case VM:
//            if (pendingData.isEmpty()) {
//              // close channnel!
//              vmScalingClientTransport.disconnect(remoteAddress, Constants.VM_WORKER_PORT);
//              localByteInputContext.receivePendingAck();
//            }
//            pendingData.add(element);
//            break;
//          default:
//            throw new UnsupportedOperationException("Unsupported type " + sendDataTo);
//        }
//      } else {
//        // if there are pending data,
//        if (!pendingData.isEmpty()) {
//          // just add it to objectQueue
//          objectQueue.addAll(pendingData);
//          pendingData.clear();
//
//        } else if (!pendingByteBufData.isEmpty()) {
//          // send it to remote VM
//          for (final ByteBuf byteBuf : pendingByteBufData) {
//            sendByteBufToRemote(byteBuf);
//          }
//          pendingByteBufData.clear();
//
//        }
//
//        switch (sendDataTo) {
//          case SF:
//            sendByteBufToRemote(serializeElement(element, serializer, edgeId, opId));
//            break;
//          case VM:
//            objectQueue.add(element);
//            break;
//          default:
//            throw new RuntimeException("Not supported type: " + sendDataTo);
//        }
//      }
//    }
//
//    @Override
//    public void flush() {
//      //channel.flush();
//    }
//  }
//
//  private void sendByteBufToRemote(final ByteBuf byteBuf) {
//    LOG.info("Scaling: Send to {}", vmTaskId);
//
//    final ByteBuf buf = vmChannel.alloc().buffer();
//    final ByteBufOutputStream bos = new ByteBufOutputStream(buf);
//    final DataOutputStream dos = new DataOutputStream(bos);
//    try {
//      dos.writeUTF(vmTaskId);
//    } catch (IOException e) {
//      e.printStackTrace();
//      throw new RuntimeException(e);
//    }
//    final CompositeByteBuf compositeByteBuf =
//      vmChannel.alloc().compositeBuffer(2).addComponents(
//        true, buf, (ByteBuf) byteBuf);
//    vmChannel.writeAndFlush(new OffloadingEvent(OffloadingEvent.Type.DATA, compositeByteBuf));
//  }
}
