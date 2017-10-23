/*
 * Copyright (C) 2017 Seoul National University
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
package edu.snu.onyx.runtime.executor.data.partitiontransfer;

import edu.snu.onyx.common.coder.Coder;
import edu.snu.onyx.compiler.ir.Element;
import edu.snu.onyx.runtime.common.comm.ControlMessage;
import edu.snu.onyx.runtime.executor.data.FileArea;
import edu.snu.onyx.runtime.executor.data.HashRange;
import edu.snu.onyx.runtime.executor.data.PartitionStore;
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
import java.util.Optional;
import java.util.concurrent.ExecutorService;

/**
 * Output stream for partition transfer. {@link #close()} must be called after finishing write.
 *
 * Encodes and flushes outbound data elements to other executors. Three threads are involved.
 * <ul>
 *   <li>User thread writes {@link Element}s or {@link FileArea}s to this object</li>
 *   <li>{@link PartitionTransfer#outboundExecutorService} encodes {@link Element}s into {@link ByteBuf}s</li>
 *   <li>Netty {@link io.netty.channel.EventLoopGroup} responds to {@link Channel#writeAndFlush(Object)}
 *   by sending {@link ByteBuf}s or {@link FileRegion}s to the remote executor.</li>
 * </ul>
 *
 * @param <T> the type of element
 */
public final class PartitionOutputStream<T> implements AutoCloseable, PartitionStream {

  private static final Logger LOG = LoggerFactory.getLogger(PartitionOutputStream.class);

  private final String receiverExecutorId;
  private final boolean encodePartialPartition;
  private final Optional<Class<? extends PartitionStore>> partitionStore;
  private final String partitionId;
  private final String runtimeEdgeId;
  private final HashRange hashRange;
  private ControlMessage.PartitionTransferType transferType;
  private short transferId;
  private Channel channel;
  private Coder<T, ?, ?> coder;
  private ExecutorService executorService;
  private int bufferSize;

  private final DataFrameWriteFutureListener writeFutureListener = new DataFrameWriteFutureListener();
  private final ClosableBlockingQueue<Object> elementQueue = new ClosableBlockingQueue<>();
  private volatile boolean closed = false;
  private volatile Throwable channelException = null;
  private volatile boolean started = false;

  @Override
  public String toString() {
    return String.format("PartitionOutputStream(%s of %s%s to %s, %s, encodePartial: %b)",
        hashRange.toString(), partitionId, partitionStore.isPresent() ? " in " + partitionStore.get() : "",
        receiverExecutorId, runtimeEdgeId, encodePartialPartition);
  }

  /**
   * Creates a partition output stream.
   *
   * @param receiverExecutorId      the id of the remote executor
   * @param encodePartialPartition  whether to start encoding even when the whole partition has not been written
   * @param partitionStore          the partition store
   * @param partitionId             the partition id
   * @param runtimeEdgeId           the runtime edge id
   * @param hashRange               the hash range
   */
  PartitionOutputStream(final String receiverExecutorId,
                        final boolean encodePartialPartition,
                        final Optional<Class<? extends PartitionStore>> partitionStore,
                        final String partitionId,
                        final String runtimeEdgeId,
                        final HashRange hashRange) {
    this.receiverExecutorId = receiverExecutorId;
    this.encodePartialPartition = encodePartialPartition;
    this.partitionStore = partitionStore;
    this.partitionId = partitionId;
    this.runtimeEdgeId = runtimeEdgeId;
    this.hashRange = hashRange;
  }

  /**
   * Sets transfer type, transfer id, and {@link io.netty.channel.Channel}.
   *
   * @param type  the transfer type
   * @param id    the transfer id
   * @param ch    the channel
   */
  void setTransferIdAndChannel(final ControlMessage.PartitionTransferType type, final short id, final Channel ch) {
    this.transferType = type;
    this.transferId = id;
    this.channel = ch;
  }

  /**
   * Sets {@link Coder}, {@link ExecutorService} and sizes to serialize bytes into partition.
   *
   * @param cdr     the coder
   * @param service the executor service
   * @param bSize   the outbound buffer size
   */
  void setCoderAndExecutorServiceAndBufferSize(final Coder<T, ?, ?> cdr,
                                               final ExecutorService service,
                                               final int bSize) {
    this.coder = cdr;
    this.executorService = service;
    this.bufferSize = bSize;
  }

  @Override
  public String getRemoteExecutorId() {
    return receiverExecutorId;
  }

  @Override
  public boolean isEncodePartialPartitionEnabled() {
    return encodePartialPartition;
  }

  @Override
  public Optional<Class<? extends PartitionStore>> getPartitionStore() {
    return partitionStore;
  }

  @Override
  public String getPartitionId() {
    return partitionId;
  }

  @Override
  public String getRuntimeEdgeId() {
    return runtimeEdgeId;
  }

  @Override
  public HashRange getHashRange() {
    return hashRange;
  }

  /**
   * Starts the encoding and writing to the channel.
   */
  private void startEncodingThreadIfNeeded() {
    if (started) {
      return;
    }
    started = true;
    assert (channel != null);
    assert (coder != null);
    final ByteBufOutputStream byteBufOutputStream = new ByteBufOutputStream();
    executorService.submit(() -> {
      try {
        final long startTime = System.currentTimeMillis();
        while (true) {
          final Object thing = elementQueue.take();
          if (thing == null) {
            // end of output stream
            byteBufOutputStream.close();
            break;
          } else if (thing instanceof Iterable) {
            final Iterable<Element> iterable = (Iterable<Element>) thing;
            for (final Element element : iterable) {
              coder.encode(element, byteBufOutputStream);
            }
          } else if (thing instanceof FileArea) {
            byteBufOutputStream.writeFileArea(false, (FileArea) thing);
          } else if (thing instanceof byte[]) {
            byteBufOutputStream.write((byte[]) thing);
          } else {
            coder.encode((Element) thing, byteBufOutputStream);
          }
        }
        final long endTime = System.currentTimeMillis();
        // If encodePartialPartition option is on, the elapsed time is not only determined by the speed of encoder
        // but also by the rate the user writes objects to this stream.
        // Before investigating on low rate of decoding, check the rate of the byte stream.
        LOG.debug("Encoding task took {} ms to complete for {} ({} bytes, buffer size: {})",
            new Object[]{endTime - startTime, toString(), byteBufOutputStream.streamLength, bufferSize});
      } catch (final Exception e) {
        LOG.error(String.format("An exception in encoding thread for %s", toString()), e);
        throw new RuntimeException(e);
      }
    });
  }

  /**
   * Sets a channel exception.
   *
   * @param cause the cause of exception handling
   */
  void onExceptionCaught(final Throwable cause) {
    LOG.error(String.format("A channel exception was set on %s", toString()), cause);
    this.channelException = cause;
  }

  /**
   * Writes an {@link Element}.
   *
   * @param element the {@link Element} to write
   * @return {@link PartitionOutputStream} (i.e. {@code this})
   * @throws IOException if an exception was set
   * @throws IllegalStateException if this stream is closed already
   */
  public PartitionOutputStream writeElement(final Element<T, ?, ?> element) throws IOException {
    checkWritableCondition();
    elementQueue.put(element);
    if (encodePartialPartition) {
      startEncodingThreadIfNeeded();
    }
    return this;
  }

  /**
   * Writes a {@link Iterable} of {@link Element}s.
   *
   * @param iterable  the {@link Iterable} to write
   * @return {@link PartitionOutputStream} (i.e. {@code this})
   * @throws IOException if an exception was set
   * @throws IllegalStateException if this stream is closed already
   */
  public PartitionOutputStream writeElements(final Iterable iterable) throws IOException {
    checkWritableCondition();
    elementQueue.put(iterable);
    if (encodePartialPartition) {
      startEncodingThreadIfNeeded();
    }
    return this;
  }

  /**
   * Writes a {@link FileArea}. Zero-copy transfer is used if possible.
   *
   * @param fileArea  provides the descriptor of the file to write
   * @return {@link PartitionOutputStream} (i.e. {@code this})
   * @throws IOException if an exception was set
   * @throws IllegalStateException if this stream is closed already
   */
  public PartitionOutputStream writeFileArea(final FileArea fileArea) throws IOException {
    checkWritableCondition();
    elementQueue.put(fileArea);
    if (encodePartialPartition) {
      startEncodingThreadIfNeeded();
    }
    return this;
  }

  /**
   * Writes a {@link Iterable} of {@link FileArea}s. Zero-copy transfer is used if possible.
   *
   * @param fileAreas the list of the file areas
   * @return {@link PartitionOutputStream} (i.e. {@code this})
   * @throws IOException if an exception was set
   * @throws IllegalStateException if this stream is closed already
   */
  public PartitionOutputStream writeFileAreas(final Iterable<FileArea> fileAreas) throws IOException {
    checkWritableCondition();
    for (final FileArea fileArea : fileAreas) {
      elementQueue.put(fileArea);
    }
    if (encodePartialPartition) {
      startEncodingThreadIfNeeded();
    }
    return this;
  }

  /**
   * Writes an array of bytes.
   *
   * @param bytes bytes to write
   * @return {@link PartitionOutputStream} (i.e. {@code this})
   * @throws IOException if an exception was set
   * @throws IllegalStateException if this stream is closed already
   */
  public PartitionOutputStream writeByteArray(final byte[] bytes) throws IOException {
    checkWritableCondition();
    elementQueue.put(bytes);
    if (encodePartialPartition) {
      startEncodingThreadIfNeeded();
    }
    return this;
  }

  /**
   * Writes a collection of arrays of bytes.
   *
   * @param byteArrays the collection of arrays of bytes to write
   * @return {@link PartitionOutputStream} (i.e. {@code this})
   * @throws IOException if an exception was set
   * @throws IllegalStateException if this stream is closed already
   */
  public PartitionOutputStream writeByteArrays(final Iterable<byte[]> byteArrays) throws IOException {
    checkWritableCondition();
    byteArrays.forEach(elementQueue::put);
    if (encodePartialPartition) {
      startEncodingThreadIfNeeded();
    }
    return this;
  }

  /**
   * Closes this stream.
   *
   * @throws IOException if an exception was set
   * @throws IllegalStateException if this stream is closed already
   */
  @Override
  public void close() throws IOException {
    if (channelException != null) {
      throw new IOException(channelException);
    }
    closed = true;
    startEncodingThreadIfNeeded();
    elementQueue.close();
  }

  /**
   * Closes this stream, exceptionally.
   *
   * @param cause the cause of the exceptional control flow
   */
  public void closeExceptionally(final Throwable cause) {
    LOG.error(String.format("Setting a user exception and closing %s and its channel", toString()), cause);
    if (closed) {
      LOG.error("Cannot close {} exceptionally because it's already closed", toString());
      throw new IllegalStateException(String.format("%s is closed already", toString()));
    }
    closed = true;
    elementQueue.close();
    channelException = cause;
    channel.close();
  }

  /**
   * Throws an {@link IOException} if needed.
   *
   * @throws IOException if an exception was set
   * @throws IllegalStateException if this stream is closed already
   */
  private void checkWritableCondition() throws IOException {
    if (channelException != null) {
      throw new IOException(channelException);
    }
    if (closed) {
      throw new IllegalStateException(String.format("%s is closed", toString()));
    }
  }

  /**
   * An {@link OutputStream} implementation which buffers data to {@link ByteBuf}s.
   */
  private final class ByteBufOutputStream extends OutputStream {

    @Nullable
    private ByteBuf byteBuf = null;

    private long streamLength = 0;

    @Override
    public void write(final int i) {
      createByteBufIfNeeded();
      byteBuf.writeByte(i);
      flushIfFull();
      streamLength++;
    }

    @Override
    public void write(final byte[] bytes, final int offset, final int length) {
      int cursor = offset;
      int bytesToWrite = length;
      while (bytesToWrite > 0) {
        createByteBufIfNeeded();
        final int toWrite = Math.min(bytesToWrite, byteBuf.writableBytes());
        byteBuf.writeBytes(bytes, cursor, toWrite);
        cursor += toWrite;
        bytesToWrite -= toWrite;
        flushIfFull();
      }
      streamLength += length;
    }

    /**
     * Writes a data frame from {@link ByteBuf} and creates a new one.
     */
    @Override
    public void flush() {
      if (byteBuf != null && byteBuf.readableBytes() > 0) {
        writeDataFrame(false);
      }
    }

    @Override
    public void close() {
      // should send a frame with "isLastFrame" on to indicate the end of the partition stream
      writeDataFrame(true);
      if (byteBuf != null) {
        byteBuf.release();
        byteBuf = null;
      }
    }

    /**
     * Flushes the buffer if the buffer is full.
     */
    private void flushIfFull() {
      if (byteBuf != null && byteBuf.writableBytes() == 0) {
        writeDataFrame(false);
      }
    }

    /**
     * Writes a data frame from {@link ByteBuf}.
     *
     * @param isLastFrame whether or not the frame is the last frame
     */
    private void writeDataFrame(final boolean isLastFrame) {
      if (byteBuf != null && byteBuf.readableBytes() > 0) {
        channel.writeAndFlush(DataFrameEncoder.DataFrame.newInstance(transferType, isLastFrame, transferId,
            byteBuf.readableBytes(), byteBuf)).addListener(writeFutureListener);
        byteBuf = null;
      } else {
        channel.writeAndFlush(DataFrameEncoder.DataFrame.newInstance(transferType, isLastFrame, transferId,
            0, null)).addListener(writeFutureListener);
      }
    }

    /**
     * Creates {@link ByteBuf} if needed.
     */
    private void createByteBufIfNeeded() {
      if (byteBuf == null) {
        byteBuf = channel.alloc().ioBuffer(bufferSize, bufferSize);
      }
    }

    /**
     * Writes a data frame from {@link FileArea}.
     *
     * @param isLastFrame whether or not the frame is the last frame
     * @param fileArea    the {@link FileArea} to transfer
     * @throws IOException when failed to open the file
     */
    private void writeFileArea(final boolean isLastFrame, final FileArea fileArea) throws IOException {
      flush();
      final Path path = Paths.get(fileArea.getPath());
      long cursor = fileArea.getPosition();
      long bytesToSend = fileArea.getCount();
      while (bytesToSend > 0) {
        final long size = Math.min(bytesToSend, DataFrameEncoder.LENGTH_MAX);
        final FileRegion fileRegion = new DefaultFileRegion(FileChannel.open(path), cursor, size);
        channel.writeAndFlush(DataFrameEncoder.DataFrame.newInstance(transferType, isLastFrame, transferId,
            size, fileRegion)).addListener(writeFutureListener);
        cursor += size;
        bytesToSend -= size;
      }
      streamLength += fileArea.getCount();
    }
  }

  /**
   * {@link ChannelFutureListener} for handling outbound exceptions on writing data frames.
   */
  private final class DataFrameWriteFutureListener implements ChannelFutureListener {

    @Override
    public void operationComplete(final ChannelFuture future) {
      if (future.isSuccess()) {
        return;
      }
      channel.close();
      if (future.cause() == null) {
        LOG.error("Failed to write a data frame");
      } else {
        onExceptionCaught(future.cause());
        LOG.error("Failed to write a data frame", future.cause());
      }
    }
  }
}
