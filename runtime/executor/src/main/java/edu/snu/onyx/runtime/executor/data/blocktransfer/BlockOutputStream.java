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
package edu.snu.onyx.runtime.executor.data.blocktransfer;

import edu.snu.onyx.common.coder.Coder;
import edu.snu.onyx.common.ir.edge.executionproperty.DataStoreProperty;
import edu.snu.onyx.runtime.common.comm.ControlMessage;
import edu.snu.onyx.runtime.common.data.KeyRange;
import edu.snu.onyx.runtime.executor.data.FileArea;
import edu.snu.onyx.runtime.executor.data.SerializedPartition;
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
import java.util.concurrent.Future;

/**
 * Output stream for block transfer. {@link #close()} must be called after finishing write.
 *
 * Encodes and flushes outbound data elements to other executors. Three threads are involved.
 * <ul>
 *   <li>User thread writes elements or {@link FileArea}s to this object</li>
 *   <li>{@link BlockTransfer#outboundExecutorService} encodes elements into {@link ByteBuf}s</li>
 *   <li>Netty {@link io.netty.channel.EventLoopGroup} responds to {@link Channel#writeAndFlush(Object)}
 *   by sending {@link ByteBuf}s or {@link FileRegion}s to the remote executor.</li>
 * </ul>
 *
 * @param <T> the type of element
 */
public final class BlockOutputStream<T> implements AutoCloseable, BlockStream {

  private static final Logger LOG = LoggerFactory.getLogger(BlockOutputStream.class);

  private final String receiverExecutorId;
  private final boolean encodePartialBlock;
  private final Optional<DataStoreProperty.Value> blockStoreValue;
  private final String blockId;
  private final String runtimeEdgeId;
  private final KeyRange keyRange;
  private ControlMessage.BlockTransferType transferType;
  private short transferId;
  private Channel channel;
  private Coder<T> coder;
  private ExecutorService executorService;
  private int bufferSize;

  private final DataFrameWriteFutureListener writeFutureListener = new DataFrameWriteFutureListener();
  private final ClosableBlockingQueue<Object> elementQueue = new ClosableBlockingQueue<>();
  private volatile boolean closed = false;
  private volatile Throwable channelException = null;
  private volatile boolean started = false;
  private volatile Future encodingThread;

  @Override
  public String toString() {
    return String.format("BlockOutputStream(%s of %s%s to %s, %s, encodePartial: %b)",
        keyRange.toString(), blockId, blockStoreValue.isPresent() ? " in " + blockStoreValue.get() : "",
        receiverExecutorId, runtimeEdgeId, encodePartialBlock);
  }

  /**
   * Creates a block output stream.
   *
   * @param receiverExecutorId the id of the remote executor
   * @param encodePartialBlock whether to start encoding even when the whole block has not been written
   * @param blockStoreValue    the block store
   * @param blockId            the block id
   * @param runtimeEdgeId      the runtime edge id
   * @param keyRange          the key range
   */
  BlockOutputStream(final String receiverExecutorId,
                    final boolean encodePartialBlock,
                    final Optional<DataStoreProperty.Value> blockStoreValue,
                    final String blockId,
                    final String runtimeEdgeId,
                    final KeyRange keyRange) {
    this.receiverExecutorId = receiverExecutorId;
    this.encodePartialBlock = encodePartialBlock;
    this.blockStoreValue = blockStoreValue;
    this.blockId = blockId;
    this.runtimeEdgeId = runtimeEdgeId;
    this.keyRange = keyRange;
  }

  /**
   * Sets transfer type, transfer id, and {@link io.netty.channel.Channel}.
   *
   * @param type the transfer type
   * @param id   the transfer id
   * @param ch   the channel
   */
  void setTransferIdAndChannel(final ControlMessage.BlockTransferType type, final short id, final Channel ch) {
    this.transferType = type;
    this.transferId = id;
    this.channel = ch;
  }

  /**
   * Sets {@link Coder}, {@link ExecutorService} and sizes to serialize bytes into block.
   *
   * @param cdr     the coder
   * @param service the executor service
   * @param bSize   the outbound buffer size
   */
  void setCoderAndExecutorServiceAndBufferSize(final Coder<T> cdr,
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
  public boolean isEncodePartialBlockEnabled() {
    return encodePartialBlock;
  }

  @Override
  public Optional<DataStoreProperty.Value> getBlockStore() {
    return blockStoreValue;
  }

  @Override
  public String getBlockId() {
    return blockId;
  }

  @Override
  public String getRuntimeEdgeId() {
    return runtimeEdgeId;
  }

  public KeyRange getKeyRange() {
    return keyRange;
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
    encodingThread = executorService.submit(() -> {
      try {
        final long startTime = System.currentTimeMillis();
        while (true) {
          final Object thing = elementQueue.take();
          if (thing == null) {
            // end of output stream
            byteBufOutputStream.close();
            break;
          } else if (thing instanceof Iterable) {
            for (final T element : (Iterable<T>) thing) {
              coder.encode(element, byteBufOutputStream);
            }
          } else if (thing instanceof FileArea) {
            byteBufOutputStream.writeFileArea((FileArea) thing);
          } else if (thing instanceof SerializedPartition) {
            byteBufOutputStream.write(
                ((SerializedPartition) thing).getData(), 0, ((SerializedPartition) thing).getLength());
          } else {
            coder.encode((T) thing, byteBufOutputStream);
          }
        }
        final long endTime = System.currentTimeMillis();
        // If encodePartialBlock option is on, the elapsed time is not only determined by the speed of encoder
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
   * Writes a {@link Iterable} of elements.
   *
   * @param iterable the {@link Iterable} to write
   * @return {@link BlockOutputStream} (i.e. {@code this})
   * @throws IOException           if an exception was set
   * @throws IllegalStateException if this stream is closed already
   */
  public BlockOutputStream writeElements(final Iterable iterable) throws IOException {
    checkWritableCondition();
    elementQueue.put(iterable);
    if (encodePartialBlock) {
      startEncodingThreadIfNeeded();
    }
    return this;
  }

  /**
   * Writes a {@link Iterable} of {@link FileArea}s. Zero-copy transfer is used if possible.
   *
   * @param fileAreas the list of the file areas
   * @return {@link BlockOutputStream} (i.e. {@code this})
   * @throws IOException           if an exception was set
   * @throws IllegalStateException if this stream is closed already
   */
  public BlockOutputStream writeFileAreas(final Iterable<FileArea> fileAreas) throws IOException {
    checkWritableCondition();
    for (final FileArea fileArea : fileAreas) {
      elementQueue.put(fileArea);
    }
    if (encodePartialBlock) {
      startEncodingThreadIfNeeded();
    }
    return this;
  }

  /**
   * Writes a collection of {@link SerializedPartition}s.
   *
   * @param serializedPartitions the collection of {@link SerializedPartition}
   * @return {@link BlockOutputStream} (i.e. {@code this})
   * @throws IOException           if an exception was set
   * @throws IllegalStateException if this stream is closed already
   */
  public BlockOutputStream writeSerializedPartitions(final Iterable<SerializedPartition> serializedPartitions)
      throws IOException {
    checkWritableCondition();
    serializedPartitions.forEach(elementQueue::put);
    if (encodePartialBlock) {
      startEncodingThreadIfNeeded();
    }
    return this;
  }

  /**
   * Closes this stream.
   *
   * @throws IOException           if an exception was set
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
   * @throws IOException           if an exception was set
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
      // should send a frame with "isLastFrame" on to indicate the end of the block stream
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
     * @param fileArea the {@link FileArea} to transfer
     * @throws IOException when failed to open the file
     */
    private void writeFileArea(final FileArea fileArea) throws IOException {
      flush();
      final Path path = Paths.get(fileArea.getPath());
      long cursor = fileArea.getPosition();
      long bytesToSend = fileArea.getCount();
      while (bytesToSend > 0) {
        final long size = Math.min(bytesToSend, DataFrameEncoder.LENGTH_MAX);
        final FileRegion fileRegion = new DefaultFileRegion(FileChannel.open(path), cursor, size);
        channel.writeAndFlush(DataFrameEncoder.DataFrame.newInstance(transferType, false, transferId,
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
      if (encodingThread != null) {
        encodingThread.cancel(true);
      }
      closed = true;
      elementQueue.close();
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
