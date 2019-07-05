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
package org.apache.nemo.runtime.executor.datatransfer;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufOutputStream;
import io.netty.channel.Channel;
import org.apache.nemo.common.coder.EncoderFactory;
import org.apache.nemo.offloading.common.EventHandler;
import org.apache.nemo.runtime.executor.common.Serializer;
import org.apache.nemo.runtime.executor.common.TaskLocationMap;
import org.apache.nemo.runtime.executor.common.datatransfer.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.List;

import static org.apache.nemo.runtime.executor.common.TaskLocationMap.LOC.SF;
import static org.apache.nemo.runtime.executor.common.TaskLocationMap.LOC.VM;
import static org.apache.nemo.runtime.executor.datatransfer.RemoteByteOutputContext.Status.PENDING;


/**
 * Container for multiple output streams. Represents a transfer context on sender-side.
 *
 * <p>Public methods are thread safe,
 * although the execution order may not be linearized if they were called from different threads.</p>
 */
public final class RemoteByteOutputContext extends AbstractByteTransferContext implements ByteOutputContext {
  private static final Logger LOG = LoggerFactory.getLogger(RemoteByteOutputContext.class.getName());

  private Channel channel;
  private Channel vmChannel;
  private Channel currChannel;
  private String vmTaskId;

  private volatile boolean closed = false;

  enum Status {
    PENDING_INIT,
    PENDING,
    NO_PENDING
  }

  private Status currStatus = Status.NO_PENDING;

  private TaskLocationMap.LOC sendDataTo = VM;

  private EventHandler<Integer> ackHandler;

  private volatile boolean settingContext = false;
  private volatile boolean restarted = false;

  private String taskId;

  private final Object writeLock = new Object();

  /**
   * Creates a output context.
   *
   * @param remoteExecutorId    id of the remote executor
   * @param contextId           identifier for this context
   * @param contextDescriptor   user-provided context descriptor
   * @param contextManager      {@link ContextManager} for the channel
   */
  public RemoteByteOutputContext(final String remoteExecutorId,
                                 final ContextId contextId,
                                 final byte[] contextDescriptor,
                                 final ContextManager contextManager) {
    super(remoteExecutorId, contextId, contextDescriptor, contextManager);
    this.channel = contextManager.getChannel();
    currChannel = channel;
  }

  @Override
  public void sendMessage(final ByteTransferContextSetupMessage message,
                          final EventHandler<Integer> handler) {
    LOG.info("Send message from {}/{} to remote: {}", getContextId().getTransferIndex(), sendDataTo, message);
    ackHandler = handler;
    // send message to the upstream task!
    switch (sendDataTo) {
      case VM: {
        channel.writeAndFlush(message);
        break;
      }
      case SF: {
        vmChannel.writeAndFlush(message);
        break;
      }
    }
  }

  public void sendMessageToVM(final ByteTransferContextSetupMessage message,
                              final EventHandler<Integer> handler) {
    LOG.info("Send message to VM channel {} / {}", getContextId().getTransferIndex(), message);
    ackHandler = handler;
    channel.writeAndFlush(message);
  }

  @Override
  public void receivePendingAck() {
    ackHandler.onNext(1);
  }

  /**
   * Closes existing sub-stream (if any) and create a new sub-stream.
   * @return new {@link ByteOutputStream}
   * @throws IOException if an exception was set or this context was closed.
   */
  @Override
  public ByteOutputStream newOutputStream() throws IOException {
    ensureNoException();
    if (closed) {
      throw new IOException("Context already closed.");
    }

    return new RemoteByteOutputStream();
  }

  @Override
  public void pending(final TaskLocationMap.LOC sdt, final String tid) {

    synchronized (writeLock) {
      //sendDataTo = sdt;

      currStatus = Status.PENDING;
      taskId = tid;

      final ByteTransferContextSetupMessage message =
        new ByteTransferContextSetupMessage(getContextId().getInitiatorExecutorId(),
          getContextId().getTransferIndex(),
          getContextId().getDataDirection(),
          getContextDescriptor(),
          getContextId().isPipe(),
          ByteTransferContextSetupMessage.MessageType.ACK_FROM_PARENT_STOP_OUTPUT,
          VM,
          taskId);

      LOG.info("Ack pending to {}, change to {}, {}",sendDataTo,  sdt, message);
      currChannel.writeAndFlush(message).addListener(getChannelWriteListener());
    }
  }

  @Override
  public void scaleoutToVm(String address, String taskId) {
    throw new RuntimeException("no supported");
    /*
    final String[] split = address.split(":");
    final ChannelFuture channelFuture =
      vmScalingClientTransport.connectTo(split[0], Constants.VM_WORKER_PORT);

    remoteAddress = split[0];

    if (channelFuture.isDone()) {
      vmChannel = channelFuture.channel();
      vmTaskId = taskId;
      isPending = false;
    } else {
      channelFuture.addListener(new GenericFutureListener<Future<? super Void>>() {
        @Override
        public void operationComplete(Future<? super Void> future) throws Exception {
          vmChannel = channelFuture.channel();
          vmTaskId = taskId;
          isPending = false;
        }
      });
    }
    */
  }

  @Override
  public synchronized void scaleoutToVm(Channel channel) {
    LOG.info("Scale out to SF: {}, channel: {}", getContextId().getTransferIndex(), channel);
    settingContext = true;
    vmChannel = channel;
    currChannel = vmChannel;
    sendDataTo = SF;
    currStatus = Status.NO_PENDING;

    if (restarted) {
      restart("??");
    }
  }

  @Override
  public void scaleoutToVmWoRestart(Channel channel) {
    LOG.info("Scale out to SF wo restart: {}, channel: {}", getContextId().getTransferIndex(), channel);
     vmChannel = channel;
    currChannel = vmChannel;
    sendDataTo = SF;
    currStatus = Status.NO_PENDING;
  }

  @Override
  public synchronized void scaleInToVm(Channel c) {
    LOG.info("Scaling in to VM: {}", getContextId().getTransferIndex());
    settingContext = true;
    channel = c;
    currChannel = channel;
    sendDataTo = VM;
    currStatus = Status.NO_PENDING;

    if (restarted) {
      restart("??");
    }
  }

  @Override
  public void scaleInToVmWoRestart(Channel c) {
    LOG.info("Scaling in to VM wo restart: {}", getContextId().getTransferIndex());
    channel = c;
    currChannel = channel;
    sendDataTo = VM;
    currStatus = Status.NO_PENDING;
  }

  public Channel getChannel() {
    return channel;
  }

  @Override
  public synchronized void stop() {
    // just send stop message

    settingContext = false;

    LOG.info("Stop context {} to {}", getContextId(), sendDataTo);

    currChannel.writeAndFlush(DataFrameEncoder.DataFrame.newInstanceForStop(getContextId()))
          .addListener(getChannelWriteListener());
  }

  @Override
  public synchronized void restart(final String taskId) {
    if (settingContext) {
      final ByteTransferContextSetupMessage message =
        new ByteTransferContextSetupMessage(getContextId().getInitiatorExecutorId(),
          getContextId().getTransferIndex(),
          getContextId().getDataDirection(), getContextDescriptor(),
          getContextId().isPipe(),
          ByteTransferContextSetupMessage.MessageType.SIGNAL_FROM_PARENT_RESTARTING_OUTPUT,
          VM,
          taskId);

      LOG.info("Restart context {} to {} {}, chanel: {}", getContextId().getTransferIndex(), sendDataTo, message, currChannel);

      currChannel.writeAndFlush(message).addListener(getChannelWriteListener());
      restarted = false;
      settingContext = false;
    } else {
      restarted = true;
    }
  }

  /**
   * Closes this stream.
   *
   * @throws IOException if an exception was set
   */
  @Override
  public void close() throws IOException {
    ensureNoException();
    if (closed) {
      return;
    }
    channel.writeAndFlush(DataFrameEncoder.DataFrame.newInstance(getContextId()))
      .addListener(getChannelWriteListener());
    deregister();
    closed = true;
  }

  @Override
  public void onChannelError(@Nullable final Throwable cause) {
    setChannelError(cause);
    channel.close();
  }

  /**
   * @throws IOException when a channel exception has been set.
   */
  void ensureNoException() throws IOException {
    if (hasException()) {
      if (getException() == null) {
        throw new IOException();
      } else {
        throw new IOException(getException());
      }
    }
  }

  /**
   * An {@link OutputStream} implementation which buffers data to {@link ByteBuf}s.
   *
   * <p>Public methods are thread safe,
   * although the execution order may not be linearized if they were called from different threads.</p>
   */
  public final class RemoteByteOutputStream extends OutputStream implements ByteOutputStream {

    private volatile boolean newSubStream = true;
    private volatile boolean closed = false;
    private final List<ByteBuf> pendingByteBufs = new ArrayList<>();

    public Channel getChannel() {
      return channel;
    }

    @Override
    public void write(final int i) throws IOException {
      final ByteBuf byteBuf = channel.alloc().ioBuffer(1, 1);
      byteBuf.writeByte(i);
      writeByteBuf(byteBuf);
    }

    @Override
    public void write(final byte[] bytes, final int offset, final int length) throws IOException {
      final ByteBuf byteBuf = channel.alloc().ioBuffer(length, length);
      byteBuf.writeBytes(bytes, offset, length);
      writeByteBuf(byteBuf);
    }

    @Override
    public void close() throws IOException {
      if (closed) {
        return;
      }
      if (newSubStream) {
        // to emit a frame with new sub-stream flag
        writeDataFrame(null, 0, true);
      }

      currChannel.flush();
      closed = true;
    }

    /**
     * Writes a data frame, from {@link ByteBuf}.
     * @param byteBuf {@link ByteBuf} to write.
     */
    private void writeByteBuf(final ByteBuf byteBuf) throws IOException {
      if (byteBuf.readableBytes() > 0) {
        writeDataFrame(byteBuf, byteBuf.readableBytes(), true);
      }
    }

    /**
     * Write an element to the channel.
     * @param element element
     * @param serializer serializer
     */
    @Override
    public void writeElement(final Object element,
                             final Serializer serializer,
                             final String edgeId,
                             final String opId) {

      //LOG.info("Writing element in {} to {}/{}, pending: {}, pendingBytes: {}",
      //  getContextId().getTransferIndex(), edgeId, opId, currStatus, pendingByteBufs.size());

      final ByteBuf byteBuf = currChannel.alloc().ioBuffer();
      final ByteBufOutputStream byteBufOutputStream = new ByteBufOutputStream(byteBuf);

      try {
        final OutputStream wrapped = byteBufOutputStream;
          //DataUtil.buildOutputStream(byteBufOutputStream, serializer.getEncodeStreamChainers());
        final EncoderFactory.Encoder encoder = serializer.getEncoderFactory().create(wrapped);
        //LOG.info("Element encoder: {}", encoder);
        encoder.encode(element);
        wrapped.close();
      } catch (final IOException e) {
        e.printStackTrace();
        throw new RuntimeException(e);
      }

      synchronized (writeLock) {
        try {
          switch (currStatus) {
            case PENDING: {
              pendingByteBufs.add(byteBuf);
              break;
            }
            case NO_PENDING: {
              if (!pendingByteBufs.isEmpty()) {
                //LOG.info("[Send pending events: {}]", pendingByteBufs.size());
                for (final ByteBuf pendingByteBuf : pendingByteBufs) {
                  writeByteBuf(pendingByteBuf);
                }
                pendingByteBufs.clear();
              }
              writeByteBuf(byteBuf);
              break;
            }
            default: {
              throw new RuntimeException("Unsupported status " + currStatus);
            }
          }
        } catch (final IOException e) {
          throw new RuntimeException(e);
        }
      }
    }

    @Override
    public void flush() {
      //channel.flush();
    }

    /**
     * Writes a data frame.
     * @param body        the body or {@code null}
     * @param length      the length of the body, in bytes
     * @throws IOException when an exception has been set or this stream was closed
     */
    private void writeDataFrame(
      final Object body, final long length, final boolean openSubStream) throws IOException {
      ensureNoException();
      if (closed) {
        throw new IOException("Stream already closed.");
      }

      currChannel.writeAndFlush(DataFrameEncoder.DataFrame.newInstance(getContextId(), body, length, openSubStream))
        .addListener(getChannelWriteListener());
    }
  }
}
