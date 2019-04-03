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

import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import org.apache.nemo.runtime.executor.common.datatransfer.ByteTransferContextSetupMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.util.Objects;

/**
 * {@link ByteInputContext} and {@link ByteOutputContext}.
 */
public interface ByteTransferContext {

  ContextManager getContextManager();

  String getRemoteExecutorId();

  /**
   * @return the identifier for this transfer context.
   */
  ContextId getContextId();

  /**
   * @return user-provided context descriptor.
   */
  byte[] getContextDescriptor();

  /**
   * @return  Whether this context has exception or not.
   */
  boolean hasException();

  /**
   * @return  The exception involved with this context, or {@code null}.
   */
  Throwable getException();
  ChannelFutureListener getChannelWriteListener();

  /**
   * Handles exception.
   * @param cause the cause of exception handling
   */
  void onChannelError(@Nullable final Throwable cause);

  /**
   * Sets exception.
   * @param cause the exception to set
   */
  void setChannelError(@Nullable final Throwable cause);

  /**
   * De-registers this context from {@link ContextManager}.
   */
  void deregister();

  /**
   * Globally unique identifier of transfer context.
   */
  public static final class ContextId {
    private final String initiatorExecutorId;
    private final String partnerExecutorId;
    private final ByteTransferContextSetupMessage.ByteTransferDataDirection dataDirection;
    private final int transferIndex;
    private final boolean isPipe;

    /**
     * Create {@link ContextId}.
     * @param initiatorExecutorId id of the executor who initiated this context and issued context id
     * @param partnerExecutorId   the other executor
     * @param dataDirection       the direction of the data flow
     * @param transferIndex       an index issued by the initiator
     * @param isPipe              is a pipe context
     */
    ContextId(final String initiatorExecutorId,
              final String partnerExecutorId,
              final ByteTransferContextSetupMessage.ByteTransferDataDirection dataDirection,
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

    public ByteTransferContextSetupMessage.ByteTransferDataDirection getDataDirection() {
      return dataDirection;
    }

    public int getTransferIndex() {
      return transferIndex;
    }

    @Override
    public String toString() {
      if (dataDirection == ByteTransferContextSetupMessage.ByteTransferDataDirection.INITIATOR_SENDS_DATA) {
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
}
