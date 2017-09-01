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
import edu.snu.vortex.runtime.common.comm.ControlMessage;
import edu.snu.vortex.runtime.executor.data.HashRange;
import io.netty.channel.Channel;

import java.util.Optional;
import java.util.concurrent.ExecutorService;

/**
 * Encodes and flushes outbound data elements to other executors.
 *
 * Three threads are involved in this class.
 * <ul>
 *   <li>User thread writes {@link edu.snu.vortex.compiler.ir.Element}s or
 *   {@link edu.snu.vortex.runtime.executor.data.FileArea}s to this object (not implemented yet)</li>
 *   <li>{@link PartitionTransfer#outboundExecutorService} encodes {@link edu.snu.vortex.compiler.ir.Element}s into
 *   {@link io.netty.buffer.ByteBuf}s (not implemented yet)</li>
 *   <li>Netty {@link io.netty.channel.EventLoopGroup} responds to
 *   {@link io.netty.channel.Channel#writeAndFlush(Object)} by sending {@link io.netty.buffer.ByteBuf}s
 *   or {@link edu.snu.vortex.runtime.executor.data.FileArea}s to the remote executor.</li>
 * </ul>
 *
 * @param <T> the type of element
 */
public final class PartitionOutputStream<T> implements PartitionStream {
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
                        final Optional<Attribute> partitionStore,
                        final String partitionId,
                        final String runtimeEdgeId,
                        final HashRange hashRange) {
  }

  /**
   * Sets transfer type, transfer id, and {@link io.netty.channel.Channel}.
   *
   * @param type  the transfer type
   * @param id    the transfer id
   * @param ch    the channel
   */
  void setTransferIdAndChannel(final ControlMessage.PartitionTransferType type, final short id, final Channel ch) {
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

  /**
   * Sets a channel exception.
   *
   * @param cause the cause of exception handling
   */
  void onExceptionCaught(final Throwable cause) {
  }
}
