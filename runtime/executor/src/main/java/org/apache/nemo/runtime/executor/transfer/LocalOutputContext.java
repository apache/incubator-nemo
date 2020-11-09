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

import org.apache.nemo.common.punctuation.Finishmark;
import org.apache.nemo.runtime.executor.data.streamchainer.Serializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * This class provides a data transfer interface to the sender side when both the sender and the receiver are
 * in the same executor. Since data serialization is unnecessary, the sender sends data without serializing
 * them. A single local output context represents a data transfer between two tasks.
 */
public final class LocalOutputContext extends LocalTransferContext implements OutputContext {
  private static final Logger LOG = LoggerFactory.getLogger(LocalOutputContext.class.getName());
  private final LinkedBlockingQueue queue = new LinkedBlockingQueue();
  private boolean isClosed = false;

  /**
   * Creates a new local output context.
   * @param executorId id of the executor to which this context belong
   * @param edgeId id of the DAG edge
   * @param srcTaskIndex source task index
   * @param dstTaskIndex destination task index
   */
  public LocalOutputContext(final String executorId,
                            final String edgeId,
                            final int srcTaskIndex,
                            final int dstTaskIndex) {
    super(executorId, edgeId, srcTaskIndex, dstTaskIndex);
  }

  /**
   * Closes this local output context.
   */
  @Override
  public void close() {
    if (isClosed) {
      throw new RuntimeException("This context has already been closed");
    }
    queue.offer(Finishmark.getInstance());
    isClosed = true;
  }

  /**
   * Accessor method for the queue in this local output context.
   * @return queue to which the sender writes its data.
   * @throws RuntimeException if the context has already been closed.
   */
  public LinkedBlockingQueue getQueue() throws RuntimeException {
    if (isClosed) {
      throw new RuntimeException("The context has already been closed.");
    }
    return queue;
  }

  /**
   * Checks whether the context has been closed.
   * @return true if the context has been closed.
   */
  public boolean isClosed() {
    return isClosed;
  }

  /**
   * Creates a new output stream to which the sender sends its data.
   * @return output stream of this local output context
   */
  @Override
  public TransferOutputStream newOutputStream() {
    return new LocalOutputStream();
  }

  /**
   * Local output stream to which the sender sends its data.
   */
  private final class LocalOutputStream implements TransferOutputStream {
    public void writeElement(final Object element, final Serializer serializer) {
      if (isClosed) {
        throw new RuntimeException("This context has already been closed.");
      }
      queue.offer(element);
    }

    public void close() {
      return;
    }
  }
}
