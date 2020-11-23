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
package org.apache.nemo.runtime.executor.transfer;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufOutputStream;
import io.netty.channel.*;
import org.apache.nemo.common.coder.EncoderFactory;
import org.apache.nemo.runtime.executor.data.DataUtil;
import org.apache.nemo.runtime.executor.data.FileArea;
import org.apache.nemo.runtime.executor.data.partition.SerializedPartition;
import org.apache.nemo.runtime.executor.data.streamchainer.Serializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.concurrent.atomic.AtomicReference;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static io.netty.buffer.Unpooled.wrappedBuffer;

/**
 * Container for multiple output streams. Represents a transfer context on sender-side.
 *
 * <p>Public methods are thread safe,
 * although the execution order may not be linearized if they were called from different threads.</p>
 */
public class ByteOutputContext extends ByteTransferContext implements OutputContext {
  private static final Logger LOG = LoggerFactory.getLogger(ByteOutputContext.class.getName());

  private final Channel channel;

  private final AtomicReference<ByteOutputStream> currentByteOutputStream = new AtomicReference<>();
  private volatile boolean closed = false;

  /**
   * Creates a output context.
   *
   * @param remoteExecutorId  id of the remote executor
   * @param contextId         identifier for this context
   * @param contextDescriptor user-provided context descriptor
   * @param contextManager    {@link ContextManager} for the channel
   */
  ByteOutputContext(final String remoteExecutorId,
                    final ContextId contextId,
                    final byte[] contextDescriptor,
                    final ContextManager contextManager) {
    super(remoteExecutorId, contextId, contextDescriptor, contextManager);
    this.channel = contextManager.getChannel();
  }

  /**
   * Closes existing sub-stream (if any) and create a new sub-stream.
   *
   * @return new {@link ByteOutputStream}
   * @throws IOException if an exception was set or this context was closed.
   */
  public final ByteOutputStream newOutputStream() throws IOException {
    ensureNoException();
    if (closed) {
      throw new IOException("Context already closed.");
    }
    if (currentByteOutputStream.get() != null) {
      currentByteOutputStream.get().close();
    }
    currentByteOutputStream.set(new ByteOutputStream());
    return currentByteOutputStream.get();
  }

  /**
   * Closes this stream.
   * @throws IOException if an exception was set
   */
  public final void close() throws IOException {
    ensureNoException();
    if (closed) {
      return;
    }
    if (currentByteOutputStream.get() != null) {
      currentByteOutputStream.get().close();
    }
    channel.writeAndFlush(DataFrameEncoder.DataFrame.newInstance(getContextId()))
      .addListener(getChannelWriteListener());
    deregister();
    closed = true;
  }

  @Override
  public final void onChannelError(@Nullable final Throwable cause) {
    setChannelError(cause);
    channel.close();
  }

  /**
   * @throws IOException when a channel exception has been set.
   */
  final void ensureNoException() throws IOException {
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
  public final class ByteOutputStream implements TransferOutputStream {

    private volatile boolean newSubStream = true;
    private volatile boolean closed = false;

    /**
     * Writes {@link SerializedPartition}.
     *
     * @param serializedPartition {@link SerializedPartition} to write.
     * @param releaseOnComplete   wheter to release the partition upon completion.
     * @return {@code this}
     * @throws IOException when an exception has been set or this stream was closed
     */
    public ByteOutputStream writeSerializedPartitionBuffer(final SerializedPartition serializedPartition,
                                                           final boolean releaseOnComplete)
      throws IOException {
      if (releaseOnComplete) {
        ChannelFutureListener listener = future -> serializedPartition.release();
        writeBuffer(serializedPartition.getDirectBufferList(), Arrays.asList(listener));
      } else {
        writeBuffer(serializedPartition.getDirectBufferList(), Collections.emptyList());
      }
      return this;
    }

    /**
     * Wraps each of the {@link ByteBuffer} in the bufList to {@link ByteBuf} object
     * to write a data frame.
     *
     * @param bufList       list of {@link ByteBuffer}s to wrap.
     * @param listeners     to add.
     * @throws IOException  when fails to write the data.
     */
    void writeBuffer(final List<ByteBuffer> bufList,
                            final List<ChannelFutureListener> listeners) throws IOException {
      final ByteBuf byteBuf = wrappedBuffer(bufList.toArray(new ByteBuffer[bufList.size()]));
      writeByteBuf(byteBuf, listeners);
    }

    /**
     * Writes a data frame, from {@link ByteBuf}.
     *
     * @param byteBuf       {@link ByteBuf} to write.
     * @param listeners     to add.
     * @throws IOException  when fails to write data.
     */
    private void writeByteBuf(final ByteBuf byteBuf, final List<ChannelFutureListener> listeners) throws IOException {
      if (byteBuf.readableBytes() > 0) {
        writeDataFrame(byteBuf, byteBuf.readableBytes(), listeners);
      }
    }

    /**
     * Writes a data frame from {@link FileArea}.
     *
     * @param fileArea the {@link FileArea} to transfer
     * @return {@code this}
     * @throws IOException when failed to open the file, an exception has been set, or this stream was closed
     */
    public ByteOutputStream writeFileArea(final FileArea fileArea) throws IOException {
      final Path path = Paths.get(fileArea.getPath());
      long cursor = fileArea.getPosition();
      long bytesToSend = fileArea.getCount();
      while (bytesToSend > 0) {
        final long size = Math.min(bytesToSend, DataFrameEncoder.LENGTH_MAX);
        final FileRegion fileRegion = new DefaultFileRegion(FileChannel.open(path), cursor, size);
        writeDataFrame(fileRegion, size, Collections.emptyList());
        cursor += size;
        bytesToSend -= size;
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
        writeDataFrame(null, 0, Collections.emptyList());
      }
      closed = true;
    }

    /**
     * Write an element to the channel.
     *
     * @param element    element
     * @param serializer serializer
     */
    public void writeElement(final Object element,
                             final Serializer serializer) {
      final ByteBuf byteBuf = channel.alloc().ioBuffer();
      final ByteBufOutputStream byteBufOutputStream = new ByteBufOutputStream(byteBuf);
      try {
        final OutputStream wrapped =
          DataUtil.buildOutputStream(byteBufOutputStream, serializer.getEncodeStreamChainers());
        final EncoderFactory.Encoder encoder = serializer.getEncoderFactory().create(wrapped);
        encoder.encode(element);
        wrapped.close();

        writeByteBuf(byteBuf, Collections.emptyList());
      } catch (final IOException e) {
        throw new RuntimeException(e);
      }
    }

    /**
     * Writes a data frame.
     *
     * @param body   the body or {@code null}
     * @param length the length of the body, in bytes
     * @param listeners to add.
     * @throws IOException when an exception has been set or this stream was closed
     */
    private void writeDataFrame(final Object body, final long length,
                                final List<ChannelFutureListener> listeners) throws IOException {
      ensureNoException();
      if (closed) {
        throw new IOException("Stream already closed.");
      }
      final ChannelFuture beforeAddingGivenListener = channel
        .writeAndFlush(DataFrameEncoder.DataFrame.newInstance(getContextId(), body, length, newSubStream))
        .addListener(getChannelWriteListener());
      listeners.forEach(beforeAddingGivenListener::addListener);

      newSubStream = false;
    }
  }
}
