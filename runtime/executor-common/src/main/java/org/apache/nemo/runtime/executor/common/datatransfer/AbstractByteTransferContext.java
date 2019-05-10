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
package org.apache.nemo.runtime.executor.common.datatransfer;

import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

/**
 * {@link ByteInputContext} and {@link ByteOutputContext}.
 */
public abstract class AbstractByteTransferContext implements ByteTransferContext {

  private static final Logger LOG = LoggerFactory.getLogger(AbstractByteTransferContext.class);

  private final String remoteExecutorId;
  private final ContextId contextId;
  private final byte[] contextDescriptor;
  private final ChannelWriteFutureListener channelWriteFutureListener = new ChannelWriteFutureListener();
  private final ContextManager contextManager;

  private volatile boolean hasException = false;
  private volatile Throwable exception = null;

  /**
   * Creates a transfer context.
   * @param remoteExecutorId    id of the remote executor
   * @param contextId           identifier for this context
   * @param contextDescriptor   user-provided context descriptor
   * @param contextManager      to de-register context when this context expires
   */
  public AbstractByteTransferContext(final String remoteExecutorId,
                                     final ContextId contextId,
                                     final byte[] contextDescriptor,
                                     final ContextManager contextManager) {
    this.remoteExecutorId = remoteExecutorId;
    this.contextId = contextId;
    this.contextDescriptor = contextDescriptor;
    this.contextManager = contextManager;
  }

  public ContextManager getContextManager() {
    return contextManager;
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
   * @return  Whether this context has exception or not.
   */
  public final boolean hasException() {
    return hasException;
  }

  /**
   * @return  The exception involved with this context, or {@code null}.
   */
  public final Throwable getException() {
    return exception;
  }

  @Override
  public final String toString() {
    return contextId.toString();
  }

  /**
   * @return Listener for channel write.
   */
  @Override
  public final ChannelFutureListener getChannelWriteListener() {
    return channelWriteFutureListener;
  }

  /**
   * Handles exception.
   * @param cause the cause of exception handling
   */
  public abstract void onChannelError(@Nullable final Throwable cause);

  /**
   * Sets exception.
   * @param cause the exception to set
   */
  public final void setChannelError(@Nullable final Throwable cause) {
    if (hasException) {
      return;
    }
    hasException = true;
    cause.printStackTrace();
    LOG.error(cause.toString());
    LOG.error(String.format("A channel exception set on %s", toString())); // Not logging throwable, which isn't useful
    exception = cause;
  }

  /**
   * De-registers this context from {@link ContextManager}.
   */
  public final void deregister() {
    contextManager.onContextExpired(this);
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
