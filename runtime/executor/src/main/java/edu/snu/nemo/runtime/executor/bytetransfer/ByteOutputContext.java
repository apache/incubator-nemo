/*
 * Copyright (C) 2018 Seoul National University
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package edu.snu.nemo.runtime.executor.bytetransfer;

import edu.snu.nemo.runtime.executor.data.FileArea;
import edu.snu.nemo.runtime.executor.data.SerializedPartition;
import io.netty.buffer.ByteBuf;
import io.netty.channel.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.Paths;

/**
 * Container for multiple output streams. Represents a transfer context on sender-side.
 *
 * <p>Public methods are thread safe,
 * although the execution order may not be linearized if they were called from different threads.</p>
 */
public final class ByteOutputContext extends ByteTransferContext implements AutoCloseable {

  private static final Logger LOG = LoggerFactory.getLogger(ByteOutputContext.class);
  private final Channel channel;

  private volatile ByteOutputStream currentByteOutputStream = null;
  private volatile boolean closed = false;

  /**
   * Creates a output context.
   *
   * @param remoteExecutorId    id of the remote executor
   * @param contextId           identifier for this context
   * @param contextDescriptor   user-provided context descriptor
   * @param contextManager      {@link ContextManager} for the channel
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
   * @return new {@link ByteOutputStream}
   * @throws IOException if an exception was set or this context was closed.
   */
  public ByteOutputStream newOutputStream() throws IOException {
    ensureNoException();
    if (closed) {
      throw new IOException("Context already closed.");
    }
    if (currentByteOutputStream != null) {
      currentByteOutputStream.close();
    }
    currentByteOutputStream = new ByteOutputStream();
    return currentByteOutputStream;
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
    if (currentByteOutputStream != null) {
      currentByteOutputStream.close();
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
        LOG.error("Exception is caught without contents.");
        throw new IOException();
      } else {
        LOG.error("Exception is caught.", getException());
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
  public final class ByteOutputStream extends OutputStream {

    private volatile boolean newSubStream = true;
    private volatile boolean closed = false;

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
    public ByteOutputStream writeFileArea(final FileArea fileArea) throws IOException {
      final Path path = Paths.get(fileArea.getPath());
      long cursor = fileArea.getPosition();
      long bytesToSend = fileArea.getCount();
      while (bytesToSend > 0) {
        final long size = Math.min(bytesToSend, DataFrameEncoder.LENGTH_MAX);
        final FileRegion fileRegion = new DefaultFileRegion(FileChannel.open(path), cursor, size);
        writeDataFrame(fileRegion, size);
        cursor += size;
        bytesToSend -= size;
      }
      return this;
    }

    @Override
    public synchronized void close() throws IOException {
      if (closed) {
        return;
      }
      if (newSubStream) {
        // to emit a frame with new sub-stream flag
        writeDataFrame(null, 0);
      }
      closed = true;
    }

    /**
     * Writes a data frame, from {@link ByteBuf}.
     * @param byteBuf {@link ByteBuf} to write.
     */
    private void writeByteBuf(final ByteBuf byteBuf) throws IOException {
      if (byteBuf.readableBytes() > 0) {
        writeDataFrame(byteBuf, byteBuf.readableBytes());
      }
    }

    /**
     * Writes a data frame.
     * @param body        the body or {@code null}
     * @param length      the length of the body, in bytes
     * @throws IOException when an exception has been set or this stream was closed
     */
    private synchronized void writeDataFrame(final Object body, final long length) throws IOException {
      ensureNoException();
      if (closed) {
        throw new IOException("Stream already closed.");
      }
      channel.writeAndFlush(DataFrameEncoder.DataFrame.newInstance(getContextId(), body, length, newSubStream))
          .addListener(getChannelWriteListener());
      newSubStream = false;
    }
  }
}
