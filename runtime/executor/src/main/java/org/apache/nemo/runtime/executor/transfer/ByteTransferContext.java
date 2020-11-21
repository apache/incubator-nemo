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

import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import org.apache.nemo.runtime.common.comm.ControlMessage.ByteTransferDataDirection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.util.concurrent.atomic.AtomicReference;
import java.util.Objects;

/**
 * {@link ByteInputContext} and {@link ByteOutputContext}.
 */
public abstract class ByteTransferContext {
  private static final Logger LOG = LoggerFactory.getLogger(ByteTransferContext.class);
  private final String remoteExecutorId;
  private final ContextId contextId;
  private final byte[] contextDescriptor;
  private final ChannelWriteFutureListener channelWriteFutureListener = new ChannelWriteFutureListener();
  private final ContextManager contextManager;
  private final AtomicReference<Throwable> exception = new AtomicReference<>();

  /**
   * Creates a transfer context.
   *
   * @param remoteExecutorId  id of the remote executor
   * @param contextId         identifier for this context
   * @param contextDescriptor user-provided context descriptor
   * @param contextManager    to de-register context when this context expires
   */
  ByteTransferContext(final String remoteExecutorId,
                      final ContextId contextId,
                      final byte[] contextDescriptor,
                      final ContextManager contextManager) {
    this.remoteExecutorId = remoteExecutorId;
    this.contextId = contextId;
    this.contextDescriptor = contextDescriptor;
    this.contextManager = contextManager;
  }

  /**
   * @return the remote executor id.
   */
  public final String getRemoteExecutorId() {
    return remoteExecutorId;
  }

  /**
   * @return the identifier for this transfer context.
   */
  public final ContextId getContextId() {
    return contextId;
  }

  /**
   * @return user-provided context descriptor.
   */
  public final byte[] getContextDescriptor() {
    return contextDescriptor;
  }

  /**
   * @return Whether this context has exception or not.
   */
  public final boolean hasException() {
    return exception.get() != null;
  }

  /**
   * @return The exception involved with this context, or {@code null}.
   */
  public final Throwable getException() {
    return exception.get();
  }

  @Override
  public final String toString() {
    return contextId.toString();
  }

  /**
   * @return Listener for channel write.
   */
  final ChannelFutureListener getChannelWriteListener() {
    return channelWriteFutureListener;
  }

  /**
   * Handles exception.
   *
   * @param cause the cause of exception handling
   */
  public abstract void onChannelError(@Nullable Throwable cause);

  /**
   * Sets exception.
   *
   * @param cause the exception to set
   */
  protected final void setChannelError(@Nullable final Throwable cause) {
    if (hasException()) {
      return;
    }
    LOG.error(String.format("A channel exception set on %s", toString())); // Not logging throwable, which isn't useful
    exception.set(cause);
  }

  /**
   * De-registers this context from {@link ContextManager}.
   */
  protected final void deregister() {
    contextManager.onContextExpired(this);
  }

  /**
   * Globally unique identifier of transfer context.
   */
  static final class ContextId {
    private final String initiatorExecutorId;
    private final String partnerExecutorId;
    private final ByteTransferDataDirection dataDirection;
    private final int transferIndex;
    private final boolean isPipe;

    /**
     * Create {@link ContextId}.
     *
     * @param initiatorExecutorId id of the executor who initiated this context and issued context id
     * @param partnerExecutorId   the other executor
     * @param dataDirection       the direction of the data flow
     * @param transferIndex       an index issued by the initiator
     * @param isPipe              is a pipe context
     */
    ContextId(final String initiatorExecutorId,
              final String partnerExecutorId,
              final ByteTransferDataDirection dataDirection,
              final int transferIndex,
              final boolean isPipe) {
      this.initiatorExecutorId = initiatorExecutorId;
      this.partnerExecutorId = partnerExecutorId;
      this.dataDirection = dataDirection;
      this.transferIndex = transferIndex;
      this.isPipe = isPipe;
    }

    public String getInitiatorExecutorId() {
      return initiatorExecutorId;
    }

    public String getPartnerExecutorId() {
      return partnerExecutorId;
    }

    public boolean isPipe() {
      return isPipe;
    }

    public ByteTransferDataDirection getDataDirection() {
      return dataDirection;
    }

    public int getTransferIndex() {
      return transferIndex;
    }

    @Override
    public String toString() {
      if (dataDirection == ByteTransferDataDirection.INITIATOR_SENDS_DATA) {
        return String.format("%s--%d->%s", initiatorExecutorId, transferIndex, partnerExecutorId);
      } else {
        return String.format("%s<-%d--%s", initiatorExecutorId, transferIndex, partnerExecutorId);
      }
    }

    @Override
    public boolean equals(final Object other) {
      if (this == other) {
        return true;
      }
      if (other == null || getClass() != other.getClass()) {
        return false;
      }
      final ContextId contextId = (ContextId) other;
      return transferIndex == contextId.transferIndex
        && Objects.equals(initiatorExecutorId, contextId.initiatorExecutorId)
        && Objects.equals(partnerExecutorId, contextId.partnerExecutorId)
        && dataDirection == contextId.dataDirection;
    }

    @Override
    public int hashCode() {
      return Objects.hash(initiatorExecutorId, partnerExecutorId, dataDirection, transferIndex);
    }
  }

  /**
   * Listener for channel write.
   */
  private final class ChannelWriteFutureListener implements ChannelFutureListener {
    @Override
    public void operationComplete(final ChannelFuture future) {
      if (future.isSuccess()) {
        return;
      }
      onChannelError(future.cause());
    }
  }
}
