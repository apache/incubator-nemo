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
package edu.snu.vortex.runtime.executor.data.partitiontransfer;

import edu.snu.vortex.common.coder.Coder;
import edu.snu.vortex.compiler.ir.attribute.Attribute;
import edu.snu.vortex.runtime.executor.data.HashRange;
import io.netty.buffer.ByteBuf;

import java.util.Optional;
import java.util.concurrent.ExecutorService;

/**
 * Decodes and stores inbound data elements from other executors.
 *
 * Three threads are involved in this class.
 * <ul>
 *   <li>Netty {@link io.netty.channel.EventLoopGroup} receives data from other executors and adds them
 *   by {@link #append(ByteBuf)}</li>
 *   <li>{@link PartitionTransfer#inboundExecutorService} decodes {@link ByteBuf}s into
 *   {@link edu.snu.vortex.compiler.ir.Element}s (not implemented yet)</li>
 *   <li>User thread iterates over this object and uses {@link java.util.Iterator} for its own work
 *   (not implemented yet)</li>
 * </ul>
 *
 * @param <T> the type of element
 */
public final class PartitionInputStream<T> implements PartitionStream {
  /**
   * Creates a partition input stream.
   *
   * @param senderExecutorId        the id of the remote executor
   * @param encodePartialPartition  whether the sender should start encoding even when the whole partition has not
   *                                been written yet
   * @param partitionStore          the partition store
   * @param partitionId             the partition id
   * @param runtimeEdgeId           the runtime edge id
   * @param hashRange               the hash range
   */
  PartitionInputStream(final String senderExecutorId,
                       final boolean encodePartialPartition,
                       final Optional<Attribute> partitionStore,
                       final String partitionId,
                       final String runtimeEdgeId,
                       final HashRange hashRange) {
  }

  /**
   * Sets {@link Coder} and {@link ExecutorService} to de-serialize bytes into partition.
   *
   * @param cdr     the coder
   * @param service the executor service
   */
  void setCoderAndExecutorService(final Coder<T, ?, ?> cdr, final ExecutorService service) {
  }

  /**
   * Accepts inbound {@link ByteBuf}.
   * @param byteBuf the byte buffer
   */
  void append(final ByteBuf byteBuf) {
  }

  /**
   * Called when all the inbound {@link ByteBuf}s are received.
   */
  void markAsEnded() {
  }

  /**
   * Starts the decoding thread, if this has not started already.
   */
  void startDecodingThreadIfNeeded() {
  }

  /**
   * Reports exception and closes this stream.
   *
   * @param cause the cause of exception handling
   */
  void onExceptionCaught(final Throwable cause) {
  }

  @Override
  public String getRemoteExecutorId() {
    return "";
  }

  @Override
  public boolean isEncodePartialPartitionEnabled() {
    return false;
  }

  @Override
  public Optional<Attribute> getPartitionStore() {
    return null;
  }

  @Override
  public String getPartitionId() {
    return null;
  }

  @Override
  public String getRuntimeEdgeId() {
    return "";
  }

  @Override
  public HashRange getHashRange() {
    return null;
  }
}
