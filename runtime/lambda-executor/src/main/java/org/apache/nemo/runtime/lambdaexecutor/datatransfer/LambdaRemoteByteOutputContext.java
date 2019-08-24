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
package org.apache.nemo.runtime.lambdaexecutor.datatransfer;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufOutputStream;
import io.netty.channel.Channel;
import org.apache.nemo.common.TaskLoc;
import org.apache.nemo.common.coder.EncoderFactory;
import org.apache.nemo.offloading.common.EventHandler;
import org.apache.nemo.runtime.executor.common.ExecutorThread;
import org.apache.nemo.runtime.executor.common.Serializer;
import org.apache.nemo.runtime.executor.common.datatransfer.*;
import org.apache.nemo.runtime.executor.common.relayserverclient.RelayControlFrame;
import org.apache.nemo.runtime.executor.common.relayserverclient.RelayDataFrame;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.List;

import static org.apache.nemo.common.TaskLoc.SF;
import static org.apache.nemo.common.TaskLoc.VM;
import static org.apache.nemo.runtime.lambdaexecutor.datatransfer.LambdaRemoteByteOutputContext.Status.PENDING;

/**
 * Container for multiple output streams. Represents a transfer context on sender-side.
 *
 * <p>Public methods are thread safe,
 * although the execution order may not be linearized if they were called from different threads.</p>
 */
public final class LambdaRemoteByteOutputContext extends AbstractByteTransferContext implements ByteOutputContext {
  private static final Logger LOG = LoggerFactory.getLogger(LambdaRemoteByteOutputContext.class.getName());

  private Channel relayChannel;
  private Channel vmChannel;
  private Channel currChannel;
  private String taskId;

  private volatile boolean closed = false;

  enum Status {
    PENDING_INIT,
    PENDING,
    NO_PENDING
  }

  private volatile Status currStatus = Status.NO_PENDING;

  private volatile TaskLoc sendDataTo;

  private String remoteAddress;
  private EventHandler<Integer> ackHandler;

  // if it is null, send to vm
  private String relayDst;

  private final Object writeLock = new Object();
  private ExecutorThread executorThread;

  /**
   * Creates a output context.
   *
   * @param remoteExecutorId    id of the remote executor
   * @param contextId           identifier for this context
   * @param contextDescriptor   user-provided context descriptor
   * @param contextManager      {@link ContextManager} for the channel
   */
  public LambdaRemoteByteOutputContext(final String remoteExecutorId,
                                       final ContextId contextId,
                                       final byte[] contextDescriptor,
                                       final ContextManager contextManager,
                                       final String relayDst,
                                       final TaskLoc sendDataTo) {
    super(remoteExecutorId, contextId, contextDescriptor, contextManager);
    this.sendDataTo = sendDataTo;
    if (sendDataTo.equals(VM)) {
      this.vmChannel = contextManager.getChannel();
      this.currChannel = vmChannel;
    } else {
      this.relayChannel = contextManager.getChannel();
      this.currChannel = relayChannel;
    }
    this.relayDst = relayDst;
    //LOG.info("RelayDst {} for remoteExecutor: {}, channel {}", relayDst, remoteExecutorId,
    //  relayChannel);
  }

  @Override
  public void sendMessage(final ByteTransferContextSetupMessage message,
                          final EventHandler<Integer> handler) {


    ackHandler = handler;
    // send message to the upstream task!

    if (sendDataTo.equals(SF)) {
      //LOG.info("Send message to {}/{}, channel: {} {}", sendDataTo,
      //  relayDst, relayChannel, message);
      relayChannel.writeAndFlush(
        new RelayControlFrame(relayDst, message));
    } else {

      //LOG.info("Send message to {}/{}, channel: {} {}", sendDataTo,
      //  relayDst, vmChannel, message);

      vmChannel.writeAndFlush(message);
    }
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
  public ByteOutputStream newOutputStream(final ExecutorThread et) throws IOException {
    ensureNoException();
    if (closed) {
      throw new IOException("Context already closed.");
    }

    executorThread = et;

    return new RemoteByteOutputStream();
  }

  @Override
  public void pending(final TaskLoc sdt, final String tid) {

    synchronized (writeLock) {
      //sendDataTo = sdt;

      taskId = tid;
      executorThread.queue.add(() -> {
        currStatus = PENDING;

        final ByteTransferContextSetupMessage message =
          new ByteTransferContextSetupMessage(getContextId().getInitiatorExecutorId(),
            getContextId().getTransferIndex(),
            getContextId().getDataDirection(),
            getContextDescriptor(),
            getContextId().isPipe(),
            ByteTransferContextSetupMessage.MessageType.ACK_FROM_PARENT_STOP_OUTPUT,
            SF,
            taskId);

        if (sdt.equals(VM)) {
          //LOG.info("Ack pending to relay {}", message);
          relayChannel.writeAndFlush(new RelayControlFrame(relayDst, message)).addListener(getChannelWriteListener());
        } else if (sdt.equals(SF)) {
          //LOG.info("Ack pending to vm {}", message);
          vmChannel.writeAndFlush(message).addListener(getChannelWriteListener());
        }
      });
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
  public void scaleoutToVm(Channel channel) {
    //LOG.info("Scale out to relay channel {}, {}", channel, getContextId());
    sendDataTo = SF;
    relayChannel = channel;
    currChannel = relayChannel;
    currStatus = Status.NO_PENDING;
    /*
    LOG.info("Scale out to channel {}", channel);
    vmChannel = channel;
    isPending = false;
    */
  }

  @Override
  public void scaleoutToVmWoRestart(Channel channel) {
      throw new RuntimeException("Not supporteD");
  }

  @Override
  public void scaleInToVm(Channel channel) {
    //LOG.info("Scale in to channel {}", channel);
    sendDataTo = VM;
    vmChannel = channel;
    currChannel = vmChannel;
    currStatus = Status.NO_PENDING;
  }

  @Override
  public void scaleInToVmWoRestart(Channel channel) {

    throw new RuntimeException("Not supporteD");
  }

  public Channel getChannel() {
    return currChannel;
  }

  @Override
  public void stop() {
    // just send stop message
    LOG.info("Stop context {}", getContextId());

    if (sendDataTo.equals(SF)) {
      relayChannel.writeAndFlush(new RelayDataFrame(relayDst, DataFrameEncoder.DataFrame.newInstanceForStop(getContextId())))
        .addListener(getChannelWriteListener());
    } else {
      vmChannel.writeAndFlush(DataFrameEncoder.DataFrame.newInstanceForStop(getContextId()))
        .addListener(getChannelWriteListener());
    }
  }

  @Override
  public void restart(final String taskId) {

    throw new RuntimeException("Not supported");

    /*
    final ByteTransferContextSetupMessage message =
      new ByteTransferContextSetupMessage(getContextId().getInitiatorExecutorId(),
        getContextId().getTransferIndex(),
        getContextId().getDataDirection(), getContextDescriptor(),
        getContextId().isPipe(),
        ByteTransferContextSetupMessage.MessageType.SIGNAL_FROM_PARENT_RESTARTING_OUTPUT,
        SF);

    LOG.info("Restart context {}", message);

    if (sendDataTo.equals(SF)) {
      relayChannel.writeAndFlush(new RelayControlFrame(relayDst, message)).addListener(getChannelWriteListener());
    } else {
      vmChannel.writeAndFlush(message).addListener(getChannelWriteListener());
    }
    */
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

    if (sendDataTo.equals(SF)) {
      relayChannel.writeAndFlush(new RelayDataFrame(relayDst, DataFrameEncoder.DataFrame.newInstance(getContextId())))
        .addListener(getChannelWriteListener());
    } else {
      vmChannel.writeAndFlush(DataFrameEncoder.DataFrame.newInstance(getContextId()))
        .addListener(getChannelWriteListener());
    }
    deregister();
    closed = true;
  }

  @Override
  public void onChannelError(@Nullable final Throwable cause) {
    setChannelError(cause);
    //channel.close();
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
      return currChannel;
    }

    @Override
    public void write(final int i) throws IOException {
      final ByteBuf byteBuf = currChannel.alloc().ioBuffer(1, 1);
      byteBuf.writeByte(i);
      writeByteBuf(byteBuf);
    }

    @Override
    public void write(final byte[] bytes, final int offset, final int length) throws IOException {
      final ByteBuf byteBuf = currChannel.alloc().ioBuffer(length, length);
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
              //LOG.info("Pending data to {}/{}, {}, {}", edgeId, opId, sendDataTo, getContextId());
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

              //LOG.info("Write data to {}/{}, {}, {}", edgeId, opId, sendDataTo, getContextId());
              writeByteBuf(byteBuf);
              break;
            }
          }
        } catch (final IOException e) {
          throw new RuntimeException(e);
        }
      }
    }

    @Override
    public void flush() {
      synchronized (writeLock) {
        currChannel.flush();
      }
    }

    /** * Writes a data frame.
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

      switch (sendDataTo) {
        case VM:
          currChannel.write(DataFrameEncoder.DataFrame.newInstance(getContextId(), body, length, openSubStream))
            .addListener(getChannelWriteListener());
          break;
        case SF:
          //LOG.info("Write data to SF channel {}", currChannel);
          currChannel.write(new RelayDataFrame(relayDst,
            DataFrameEncoder.DataFrame.newInstance(getContextId(), body, length, openSubStream)))
            .addListener(getChannelWriteListener());
          /*
          LOG.info("Scaling: Send to {}", vmTaskId);

          final ByteBuf buf = vmChannel.alloc().buffer();
          final ByteBufOutputStream bos = new ByteBufOutputStream(buf);
          final DataOutputStream dos = new DataOutputStream(bos);
          dos.writeUTF(vmTaskId);
          final ByteBuf bb = (ByteBuf) body;
          // DATA ID
          bb.writeInt(0);

          final CompositeByteBuf compositeByteBuf =
            vmChannel.alloc().compositeBuffer(2).addComponents(
              true, buf, (ByteBuf) body);
          vmChannel.writeAndFlush(new OffloadingEvent(OffloadingEvent.Type.DATA, compositeByteBuf));
          */

          break;
        default:
          throw new RuntimeException("Unsupported type: " + sendDataTo);
      }

    }
  }
}
