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
import org.apache.nemo.common.coder.EncoderFactory;
import org.apache.nemo.runtime.executor.common.Serializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.io.IOException;
import java.io.OutputStream;

/**
 * Container for multiple output streams. Represents a transfer context on sender-side.
 *
 * <p>Public methods are thread safe,
 * although the execution order may not be linearized if they were called from different threads.</p>
 */
public final class ByteOutputContext extends LambdaByteTransferContext implements AutoCloseable {
  private static final Logger LOG = LoggerFactory.getLogger(ByteOutputContext.class.getName());

  private final Channel channel;

  private volatile boolean closed = false;

  /**
   * Creates a output context.
   *
   * @param remoteExecutorId    id of the remote executor
   * @param contextId           identifier for this context
   * @param contextDescriptor   user-provided context descriptor
   */
  ByteOutputContext(final String remoteExecutorId,
                    final ContextId contextId,
                    final byte[] contextDescriptor,
                    final LambdaContextManager contextManager) {
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

    return new ByteOutputStream();
  }


  public Channel getChannel() {
    return channel;
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
    channel.writeAndFlush(LambdaDataFrameEncoder.DataFrame.newInstance(getContextId()))
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
  public final class ByteOutputStream extends OutputStream {

    private volatile boolean newSubStream = true;
    private volatile boolean closed = false;

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
    public void writeElement(final Object element,
                             final Serializer serializer) {
      final ByteBuf byteBuf = channel.alloc().ioBuffer();
      final ByteBufOutputStream byteBufOutputStream = new ByteBufOutputStream(byteBuf);
      try {
        final EncoderFactory.Encoder encoder = serializer.getEncoderFactory().create(byteBufOutputStream);
        //LOG.info("Element encoder: {}", encoder);
        encoder.encode(element);
        byteBufOutputStream.close();

        writeByteBuf(byteBuf);
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
      channel.write(LambdaDataFrameEncoder.DataFrame.newInstance(getContextId(), body, length, openSubStream))
        .addListener(getChannelWriteListener());
    }
  }
}