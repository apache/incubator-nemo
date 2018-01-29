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

import java.io.IOException;
import java.io.OutputStream;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Iterator;
import java.util.Optional;

/**
 * Output stream for block transfer. {@link #close()} must be called after finishing write.
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

  private final DataFrameWriteFutureListener writeFutureListener = new DataFrameWriteFutureListener();
  private final ByteBufOutputStream byteBufOutputStream = new ByteBufOutputStream();
  private volatile boolean closed = false;
  private volatile Throwable channelException = null;

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
   * Sets {@link Coder}.
   *
   * @param cdr     the coder
   */
  void setCoder(final Coder<T> cdr) {
    this.coder = cdr;
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
   * @param iterator the {@link Iterator} to write
   * @return {@link BlockOutputStream} (i.e. {@code this})
   * @throws IOException           if an exception was set
   * @throws IllegalStateException if this stream is closed already
   */
  public BlockOutputStream writeElements(final Iterator<T> iterator) throws IOException {
    checkWritableCondition();
    while (iterator.hasNext()) {
      coder.encode(iterator.next(), byteBufOutputStream);
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
      byteBufOutputStream.writeFileArea(fileArea);
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
    serializedPartitions.forEach(serializedPartition -> byteBufOutputStream.write(serializedPartition.getData(),
        0, serializedPartition.getLength()));
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
    byteBufOutputStream.close();
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

    @Override
    public void write(final int i) {
      final ByteBuf byteBuf = channel.alloc().ioBuffer(1, 1);
      byteBuf.writeByte(i);
      writeDataFrame(byteBuf, false);
    }

    @Override
    public void write(final byte[] bytes, final int offset, final int length) {
      final ByteBuf byteBuf = channel.alloc().ioBuffer(length, length);
      byteBuf.writeBytes(bytes, offset, length);
      writeDataFrame(byteBuf, false);
    }

    @Override
    public void close() {
      // should send a frame with "isLastFrame" on to indicate the end of the block stream
      writeDataFrame(true);
    }

    /**
     * Writes a data frame with empty body.
     *
     * @param isLastFrame whether or not the frame is the last frame
     */
    private void writeDataFrame(final boolean isLastFrame) {
      channel.writeAndFlush(DataFrameEncoder.DataFrame.newInstance(transferType, isLastFrame, transferId,
          0, null)).addListener(writeFutureListener);
    }

    /**
     * Writes a data frame from {@link ByteBuf}.
     *
     * @param byteBuf     {@link ByteBuf} to write.
     * @param isLastFrame whether or not the frame is the last frame
     */
    private void writeDataFrame(final ByteBuf byteBuf, final boolean isLastFrame) {
      if (byteBuf.readableBytes() > 0) {
        channel.writeAndFlush(DataFrameEncoder.DataFrame.newInstance(transferType, isLastFrame, transferId,
            byteBuf.readableBytes(), byteBuf)).addListener(writeFutureListener);
      } else {
        writeDataFrame(isLastFrame);
      }
    }

    /**
     * Writes a data frame from {@link FileArea}.
     *
     * @param fileArea the {@link FileArea} to transfer
     * @throws IOException when failed to open the file
     */
    private void writeFileArea(final FileArea fileArea) throws IOException {
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
      closed = true;
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
