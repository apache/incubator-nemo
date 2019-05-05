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
import org.apache.nemo.offloading.common.EventHandler;
import org.apache.nemo.runtime.executor.common.datatransfer.ByteTransferContextSetupMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.io.IOException;
import java.io.InputStream;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.concurrent.CompletableFuture;

/**
 * Container for multiple input streams. Represents a transfer context on receiver-side.
 *
 * <h3>Thread safety:</h3>
 * <p>Methods with default access modifier, namely {@link #onNewStream()}, {@link #onByteBuf(ByteBuf)},
 * {@link #onContextClose()}, are not thread-safe, since they are called by a single Netty event loop.</p>
 * <p>Public methods are thread safe,
 * although the execution order may not be linearized if they were called from different threads.</p>
 */
public interface ByteInputContext extends ByteTransferContext {

  /**
   * Returns {@link Iterator} of {@link InputStream}s.
   * This method always returns the same {@link Iterator} instance.
   * @return {@link Iterator} of {@link InputStream}s.
   */
  Iterator<InputStream> getInputStreams();

  /**
   * Returns a future, which is completed when the corresponding transfer for this context gets done.
   * @return a {@link CompletableFuture} for the same value that {@link #getInputStreams()} returns
   */
  CompletableFuture<Iterator<InputStream>> getCompletedFuture();

  /**
   * Called when a punctuation for sub-stream incarnation is detected.
   */
  void onNewStream();

  void sendMessage(final ByteTransferContextSetupMessage message,
                   final EventHandler<Integer> pendingAckHandler);

  void receivePendingAck();

  /**
   * Called when {@link ByteBuf} is supplied to this context.
   * @param byteBuf the {@link ByteBuf} to supply
   */
  void onByteBuf(final ByteBuf byteBuf);

  boolean isFinished();

  /**
   * Called when {@link #onByteBuf(ByteBuf)} event is no longer expected.
   */
  void onContextClose();

  void onContextStop();

  void onContextRestart();
}
