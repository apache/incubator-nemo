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
package org.apache.nemo.runtime.executor.data;

import org.apache.nemo.common.Pair;
import org.apache.nemo.runtime.executor.bytetransfer.ByteOutputContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.concurrent.ThreadSafe;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Writes happen in a serialized manner with {@link PipeContainer#putPipeListIfAbsent(Pair, int)}.
 * This ensures that each key is initialized exactly once, and never updated.
 * <p>
 * Writes and reads for the same key never occur concurrently with no problem, because
 * (1) write never updates, and (2) read happens only after the write.
 * <p>
 * Reads can happen concurrently with no problem.
 */
@ThreadSafe
public final class PipeContainer {
  private static final Logger LOG = LoggerFactory.getLogger(PipeContainer.class.getName());
  private final ConcurrentHashMap<Pair<String, Long>, CountBasedBlockingContainer<ByteOutputContext>> pipeMap;

  PipeContainer() {
    this.pipeMap = new ConcurrentHashMap<>();
  }

  /**
   * Blocks the get operation when the number of elements is smaller than expected.
   *
   * @param <T> type of the value.
   */
  class CountBasedBlockingContainer<T> {
    private final Map<Integer, T> indexToValue;
    private final int expected;
    private final Lock lock;
    private final Condition condition;

    CountBasedBlockingContainer(final int expected) {
      this.indexToValue = new HashMap<>(expected);
      this.expected = expected;
      this.lock = new ReentrantLock();
      this.condition = lock.newCondition();
    }

    public List<T> getValuesBlocking() {
      lock.lock();
      try {
        if (!isCountSatistified()) {
          condition.await();
        }
        return new ArrayList<>(indexToValue.values());
      } catch (InterruptedException e) {
        throw new RuntimeException(e);
      } finally {
        lock.unlock();
      }
    }

    public void setValue(final int index, final T value) {
      lock.lock();
      try {
        final T previous = indexToValue.put(index, value);
        if (null != previous) {
          throw new IllegalStateException(previous.toString());
        }

        if (isCountSatistified()) {
          condition.signalAll();
        }
      } finally {
        lock.unlock();
      }
    }

    private boolean isCountSatistified() {
      if (indexToValue.size() < expected) {
        return false;
      } else if (indexToValue.size() == expected) {
        return true;
      } else {
        throw new IllegalStateException(indexToValue.size() + " < " + expected);
      }
    }

    @Override
    public String toString() {
      return indexToValue.toString();
    }
  }

  /**
   * (SYNCHRONIZATION) Initialize the key exactly once.
   *
   * @param pairKey the pair of the runtime edge id and the source task index.
   * @param expected the expected number of pipes to wait for.
   */
  synchronized void putPipeListIfAbsent(final Pair<String, Long> pairKey, final int expected) {
    pipeMap.putIfAbsent(pairKey, new CountBasedBlockingContainer(expected));
  }

  /**
   * (SYNCHRONIZATION) CountBasedBlockingContainer takes care of it.
   *
   * @param pairKey the pair of the runtime edge id and the source task index.
   * @param dstTaskIndex the destination task index.
   * @param byteOutputContext the byte output context.
   */
  void putPipe(final Pair<String, Long> pairKey, final int dstTaskIndex, final ByteOutputContext byteOutputContext) {
    final CountBasedBlockingContainer<ByteOutputContext> container = pipeMap.get(pairKey);
    container.setValue(dstTaskIndex, byteOutputContext);
  }

  /**
   * (SYNCHRONIZATION) CountBasedBlockingContainer takes care of it.
   *
   * @param pairKey the pair of the runtime edge id and the source task index.
   * @return the list of byte output context.
   */
  List<ByteOutputContext> getPipes(final Pair<String, Long> pairKey) {
    final CountBasedBlockingContainer<ByteOutputContext> container = pipeMap.get(pairKey);
    return container.getValuesBlocking();
  }
}

