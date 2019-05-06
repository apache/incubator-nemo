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
import io.netty.buffer.ByteBufOutputStream;
import io.netty.buffer.CompositeByteBuf;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.DefaultFileRegion;
import io.netty.channel.FileRegion;
import io.netty.util.concurrent.Future;
import io.netty.util.concurrent.GenericFutureListener;
import org.apache.nemo.common.coder.EncoderFactory;
import org.apache.nemo.offloading.common.Constants;
import org.apache.nemo.offloading.common.OffloadingEvent;
import org.apache.nemo.runtime.executor.common.Serializer;
import org.apache.nemo.runtime.executor.common.datatransfer.ByteTransferContextSetupMessage;
import org.apache.nemo.runtime.executor.data.DataUtil;
import org.apache.nemo.runtime.executor.data.FileArea;
import org.apache.nemo.runtime.executor.data.partition.SerializedPartition;
import org.apache.nemo.runtime.executor.datatransfer.VMScalingClientTransport;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

import static org.apache.nemo.runtime.executor.bytetransfer.RemoteByteOutputContext.SendDataTo.SCALE_VM;
import static org.apache.nemo.runtime.executor.bytetransfer.RemoteByteOutputContext.SendDataTo.VM;

/**
 * Container for multiple output streams. Represents a transfer context on sender-side.
 *
 * <p>Public methods are thread safe,
 * although the execution order may not be linearized if they were called from different threads.</p>
 */
public final class RemoteByteOutputContext extends AbstractByteTransferContext implements ByteOutputContext {
  private static final Logger LOG = LoggerFactory.getLogger(RemoteByteOutputContext.class.getName());

  private final Channel channel;
  private Channel vmChannel;
  private String vmTaskId;

  private volatile boolean closed = false;
  private volatile boolean isPending = false;

  public enum SendDataTo {
    VM,
    SCALE_VM,
    SCALE_SF
  }

  private SendDataTo sendDataTo = VM;
  private final VMScalingClientTransport vmScalingClientTransport;

  private String remoteAddress;

  /**
   * Creates a output context.
   *
   * @param remoteExecutorId    id of the remote executor
   * @param contextId           identifier for this context
   * @param contextDescriptor   user-provided context descriptor
   * @param contextManager      {@link ContextManager} for the channel
   */
  RemoteByteOutputContext(final String remoteExecutorId,
                          final ContextId contextId,
                          final byte[] contextDescriptor,
                          final ContextManager contextManager,
                          final VMScalingClientTransport vmScalingClientTransport) {
    super(remoteExecutorId, contextId, contextDescriptor, contextManager);
    this.channel = contextManager.getChannel();
    this.vmScalingClientTransport = vmScalingClientTransport;
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
  public void pending(final boolean scaleout) {
    sendDataTo = scaleout ? SCALE_VM : VM;
    isPending = true;
  }

  @Override
  public void scaleoutToVm(String address, String taskId) {
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
  }

  @Override
  public void scaleInToVm() {
    isPending = false;
  }


  public Channel getChannel() {
    return channel;
  }

  @Override
  public void stop() {
    // just send stop message

    LOG.info("Stop context {}", getContextId());

    channel.writeAndFlush(DataFrameEncoder.DataFrame.newInstanceForStop(getContextId()))
      .addListener(getChannelWriteListener());
  }

  @Override
  public void restart() {
    final ByteTransferContextSetupMessage message =
      new ByteTransferContextSetupMessage(getContextId().getInitiatorExecutorId(),
        getContextId().getTransferIndex(),
        getContextId().getDataDirection(),
        getContextDescriptor(),
        getContextId().isPipe(),
        ByteTransferContextSetupMessage.MessageType.RESTART);

    LOG.info("Restart context {}", message);

    channel.writeAndFlush(message).addListener(getChannelWriteListener());
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

    /**
     * Writes {@link SerializedPartition}.
     * @param serializedPartition {@link SerializedPartition} to write.
     * @return {@code this}
     * @throws IOException when an exception has been set or this stream was closed
     */
    @Override
    public ByteOutputStream writeSerializedPartition(final SerializedPartition serializedPartition)
      throws IOException {
      write(serializedPartition.getData(), 0, serializedPartition.getLength());
      return this;
    }

    /**
     * Writes a data frame from {@link FileArea}.
     *
     * @param fileArea the {@link FileArea} to transfer
     * @return {@code this}
     * @throws IOException when failed to open the file, an exception has been set, or this stream was closed
     */
    @Override
    public ByteOutputStream writeFileArea(final FileArea fileArea) throws IOException {
      final Path path = Paths.get(fileArea.getPath());
      long cursor = fileArea.getPosition();
      long bytesToSend = fileArea.getCount();
      boolean init = true;
      while (bytesToSend > 0) {
        final long size = Math.min(bytesToSend, DataFrameEncoder.LENGTH_MAX);
        final FileRegion fileRegion = new DefaultFileRegion(FileChannel.open(path), cursor, size);
        writeDataFrame(fileRegion, size, init);
        cursor += size;
        bytesToSend -= size;
        init = false;
      }
      return this;
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

      channel.flush();
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

      final ByteBuf byteBuf = channel.alloc().ioBuffer();
      final ByteBufOutputStream byteBufOutputStream = new ByteBufOutputStream(byteBuf);

      if (sendDataTo.equals(SCALE_VM)) {
        try {
          byteBufOutputStream.writeBoolean(false);
          byteBufOutputStream.writeUTF(edgeId);
          byteBufOutputStream.writeUTF(opId);
        } catch (IOException e) {
          e.printStackTrace();
          throw new RuntimeException(e);
        }
      }

      try {
        final OutputStream wrapped =
          DataUtil.buildOutputStream(byteBufOutputStream, serializer.getEncodeStreamChainers());
        final EncoderFactory.Encoder encoder = serializer.getEncoderFactory().create(wrapped);
        //LOG.info("Element encoder: {}", encoder);
        encoder.encode(element);
        wrapped.close();
      } catch (final IOException e) {
        e.printStackTrace();
        throw new RuntimeException(e);
      }

      try {
        if (isPending) {
          // If it is pending, buffer data
          if (pendingByteBufs.isEmpty() && sendDataTo.equals(SCALE_VM)) {
            final ByteTransferContextSetupMessage message =
              new ByteTransferContextSetupMessage(getContextId().getInitiatorExecutorId(),
                getContextId().getTransferIndex(),
                getContextId().getDataDirection(),
                getContextDescriptor(),
                getContextId().isPipe(),
                ByteTransferContextSetupMessage.MessageType.ACK_PENDING);
            LOG.info("Ack pending {}", message);
            channel.writeAndFlush(message).addListener(getChannelWriteListener());
          } else if (pendingByteBufs.isEmpty() && sendDataTo.equals(VM)) {
            // close channel
             final ByteTransferContextSetupMessage message =
              new ByteTransferContextSetupMessage(getContextId().getInitiatorExecutorId(),
                getContextId().getTransferIndex(),
                getContextId().getDataDirection(),
                getContextDescriptor(),
                getContextId().isPipe(),
                ByteTransferContextSetupMessage.MessageType.ACK_PENDING);
            LOG.info("Closing vm channel {}", message);
            vmScalingClientTransport.disconnect(remoteAddress, Constants.VM_WORKER_PORT);

            LOG.info("Ack VM scaling pending {}", message);
          }

          pendingByteBufs.add(byteBuf);

        } else {

          if (!pendingByteBufs.isEmpty()) {
            for (final ByteBuf pendingByteBuf : pendingByteBufs) {
              writeByteBuf(pendingByteBuf);
            }
            pendingByteBufs.clear();
          }
          writeByteBuf(byteBuf);
        }

      } catch (final IOException e) {
        throw new RuntimeException(e);
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

      switch (sendDataTo) {
        case VM:
          channel.write(DataFrameEncoder.DataFrame.newInstance(getContextId(), body, length, openSubStream))
            .addListener(getChannelWriteListener());
          break;
        case SCALE_VM:
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

          break;
        default:
          throw new RuntimeException("Unsupported type: " + sendDataTo);
      }

    }
  }
}
