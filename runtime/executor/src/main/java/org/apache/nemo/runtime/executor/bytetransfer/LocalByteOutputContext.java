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
import io.netty.channel.Channel;
import io.netty.channel.DefaultFileRegion;
import io.netty.channel.FileRegion;
import org.apache.nemo.common.coder.EncoderFactory;
import org.apache.nemo.runtime.executor.common.Serializer;
import org.apache.nemo.runtime.executor.common.datatransfer.ByteTransferContextSetupMessage;
import org.apache.nemo.runtime.executor.data.DataUtil;
import org.apache.nemo.runtime.executor.data.FileArea;
import org.apache.nemo.runtime.executor.data.partition.SerializedPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Queue;

/**
 * Container for multiple output streams. Represents a transfer context on sender-side.
 *
 * <p>Public methods are thread safe,
 * although the execution order may not be linearized if they were called from different threads.</p>
 */
public final class LocalByteOutputContext extends AbstractByteTransferContext implements ByteOutputContext {
  private static final Logger LOG = LoggerFactory.getLogger(LocalByteOutputContext.class.getName());

  private final Channel channel;

  private volatile boolean closed = false;

  private final Queue<Object> objectQueue;

  /**
   * Creates a output context.
   *
   * @param remoteExecutorId    id of the remote executor
   * @param contextId           identifier for this context
   * @param contextDescriptor   user-provided context descriptor
   * @param contextManager      {@link ContextManager} for the channel
   */
  LocalByteOutputContext(final String remoteExecutorId,
                         final ContextId contextId,
                         final byte[] contextDescriptor,
                         final ContextManager contextManager,
                         final Queue<Object> objectQueue) {
    super(remoteExecutorId, contextId, contextDescriptor, contextManager);
    this.channel = contextManager.getChannel();
    this.objectQueue = objectQueue;
  }

  /**
   * Closes existing sub-stream (if any) and create a new sub-stream.
   * @return new {@link ByteOutputStream}
   * @throws IOException if an exception was set or this context was closed.
   */
  public ByteOutputStream newOutputStream() throws IOException {
    return new LocalByteOutputStream();
  }


  public Channel getChannel() {
    return channel;
  }

  public void stop() {
    // just send stop message
    LOG.info("Stop local context {}", getContextId());
    getContextManager().onContextStopLocal(getContextId().getTransferIndex());
  }

  public void restart() {
    LOG.info("Restart local context {}", getContextId());
    getContextManager().onContextRestartLocal(getContextId().getTransferIndex());
  }

  /**
   * Closes this stream.
   *
   * @throws IOException if an exception was set
   */
  @Override
  public void close() throws IOException {
    getContextManager().onContextCloseLocal(getContextId().getTransferIndex());
  }

  @Override
  public void onChannelError(@Nullable final Throwable cause) {
    throw new RuntimeException(cause);
  }

  /**
   * An {@link OutputStream} implementation which buffers data to {@link ByteBuf}s.
   *
   * <p>Public methods are thread safe,
   * although the execution order may not be linearized if they were called from different threads.</p>
   */
  public final class LocalByteOutputStream implements ByteOutputStream {

    private volatile boolean newSubStream = true;
    private volatile boolean closed = false;

    public Channel getChannel() {
      return channel;
    }

    @Override
    public void write(final int i) throws IOException {
      throw new UnsupportedOperationException();
    }

    @Override
    public void write(final byte[] bytes, final int offset, final int length) throws IOException {
      throw new UnsupportedOperationException();
    }

    /**
     * Writes {@link SerializedPartition}.
     * @param serializedPartition {@link SerializedPartition} to write.
     * @return {@code this}
     * @throws IOException when an exception has been set or this stream was closed
     */
    public ByteOutputStream writeSerializedPartition(final SerializedPartition serializedPartition)
      throws IOException {
      throw new UnsupportedOperationException();
    }

    /**
     * Writes a data frame from {@link FileArea}.
     *
     * @param fileArea the {@link FileArea} to transfer
     * @return {@code this}
     * @throws IOException when failed to open the file, an exception has been set, or this stream was closed
     */
    public ByteOutputStream writeFileArea(final FileArea fileArea) throws IOException {
      throw new UnsupportedOperationException();
    }

    @Override
    public void close() throws IOException {
      // do nothing
    }

    /**
     * Write an element to the channel.
     * @param element element
     * @param serializer serializer
     */
    public void writeElement(final Object element,
                             final Serializer serializer) {
      objectQueue.add(element);
    }

    @Override
    public void flush() {
      //channel.flush();
    }
  }
}
