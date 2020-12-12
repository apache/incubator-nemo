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
import java.util.Iterator;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * This class provides a data transfer interface to the receiver side when both the sender and the receiver are in the
 * same executor. Since the sender doesn't serialize data, the receiver doesn't need to deserialize data when retrieving
 * them.
 */
  public final class LocalInputContext extends LocalTransferContext {
    private final LinkedBlockingQueue queue;
    private boolean outputContextClosed = false;

    /**
     * Creates a new local input context and connect it to {@code localOutputContext}.
     * @param localOutputContext the local output context to which this local input context is connected
     */
    public LocalInputContext(final LocalOutputContext localOutputContext) {
    super(localOutputContext.getExecutorId(),
          localOutputContext.getEdgeId(),
          localOutputContext.getSrcTaskIndex(),
          localOutputContext.getDstTaskIndex());
    this.queue = localOutputContext.getQueue();
  }

  /**
   * Checks if the connected output context has already been closed. It is for testing purpose.
   * @return true if the connected output context has already been closed.
   */
  public boolean isOutputContextClosed() {
    return outputContextClosed;
  }

  /**
   * Creates a new iterator which iterates the receive elements from the sender.
   * @return iterator that iterates the received elements.
   */
  public LocalInputIterator getIterator() {
    return new LocalInputIterator();
  }

  /**
   * Local input iterator that iterates the received elements from the sender.
   */
  private class LocalInputIterator implements Iterator {
    private Object next;
    private boolean hasNext = false;

    @Override
    public final boolean hasNext() {
      if (hasNext) {
        return true;
      }
      if (outputContextClosed) {
        return false;
      }
      try {
        // Blocking call
        next = queue.take();
        if (next instanceof Finishmark) {
          outputContextClosed = true;
          return false;
        }
      } catch (InterruptedException e) {
        throw new RuntimeException(e);
      }
      hasNext = true;
      return true;
    }

    @Override
    public final Object next() throws RuntimeException {
      if (outputContextClosed) {
        throw new RuntimeException("The connected output context has already been closed");
      } else if (!hasNext) {
        throw new RuntimeException("Next element is not available");
      } else {
        hasNext = false;
        return next;
      }
    }
  }
}
