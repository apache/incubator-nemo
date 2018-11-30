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

import io.netty.buffer.ByteBufOutputStream;
import org.apache.nemo.common.coder.EncoderFactory;
import org.apache.nemo.runtime.executor.data.DataUtil;
import org.apache.nemo.runtime.executor.data.FileArea;
import org.apache.nemo.runtime.executor.data.partition.SerializedPartition;
import io.netty.buffer.ByteBuf;
import io.netty.channel.*;
import org.apache.nemo.runtime.executor.data.streamchainer.Serializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Output context for sending data to the channel. Represents a transfer context on sender-side.
 *
 * <p>Public methods are thread safe,
 * although the execution order may not be linearized if they were called from different threads.</p>
 */
public final class ByteOutputContext extends ByteTransferContext implements AutoCloseable {
  private static final Logger LOG = LoggerFactory.getLogger(ByteOutputContext.class.getName());

  private final Channel channel;
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
   * Writes {@link SerializedPartition}.
   * @param serializedPartition {@link SerializedPartition} to write.
   * @throws IOException when an exception has been set or this stream was closed
   */
  public void writeSerializedPartition(final SerializedPartition serializedPartition)
    throws IOException {
    write(serializedPartition.getData(), 0, serializedPartition.getLength());
  }

  /**
   * Write an element to the channel.
   * @param element element
   * @param serializer serializer for the element
   */
  public void writeElement(final Object element,
                           final Serializer serializer) {
    final ByteBufOutputStream byteBufOutputStream = new ByteBufOutputStream(channel.alloc().ioBuffer());
    try {
      final OutputStream wrapped = DataUtil.buildOutputStream(
        byteBufOutputStream, serializer.getEncodeStreamChainers());
      final EncoderFactory.Encoder encoder = serializer.getEncoderFactory().create(wrapped);
      encoder.encode(element);
      wrapped.close();
      writeByteBuf(byteBufOutputStream.buffer());
    } catch (IOException e) {
      e.printStackTrace();
      throw new RuntimeException(e);
    }
  }

  /**
   * Writes a data frame from {@link FileArea}.
   *
   * @param fileArea the {@link FileArea} to transfer
   * @throws IOException when failed to open the file, an exception has been set, or this stream was closed
   */
  public void writeFileArea(final FileArea fileArea) throws IOException {
    final Path path = Paths.get(fileArea.getPath());
    long cursor = fileArea.getPosition();
    long bytesToSend = fileArea.getCount();
    boolean init = true;
    while (bytesToSend > 0) {
      final long size = Math.min(bytesToSend, DataFrameEncoder.LENGTH_MAX);
      final FileRegion fileRegion = new DefaultFileRegion(FileChannel.open(path), cursor, size);
      writeDataFrame(fileRegion, size, init);
      init = false;
      cursor += size;
      bytesToSend -= size;
    }
  }

  private void write(final byte[] bytes, final int offset, final int length) throws IOException {
    final ByteBuf byteBuf = channel.alloc().ioBuffer(length, length);
    byteBuf.writeBytes(bytes, offset, length);
    writeByteBuf(byteBuf);
  }

  /**
   * Writes a data frame.
   * @param body        the body or {@code null}
   * @param length      the length of the body, in bytes
   * @throws IOException when an exception has been set or this stream was closed
   */
  private synchronized void writeDataFrame(final Object body,
                                           final long length,
                                           final boolean newSubStream) throws IOException {
    ensureNoException();
    if (closed) {
      throw new IOException("Stream already closed.");
    }
    channel.writeAndFlush(DataFrameEncoder.DataFrame.newInstance(getContextId(), body, length, newSubStream))
      .addListener(getChannelWriteListener());

    if (newSubStream) {
      // to emit a frame with new sub-stream flag
      channel.writeAndFlush(DataFrameEncoder.DataFrame.newInstance(getContextId(), null, 0, true))
        .addListener(getChannelWriteListener());
    }
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
   * Closes this stream.
   *
   * @throws IOException if an exception was set
   */
  @Override
  public synchronized void close() throws IOException {
    ensureNoException();
    if (!closed) {
      closed = true;
      channel.writeAndFlush(DataFrameEncoder.DataFrame.newInstance(getContextId()))
        .addListener(getChannelWriteListener());
      deregister();
    }
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
}
