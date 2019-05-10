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
import org.apache.nemo.runtime.executor.common.datatransfer.ByteTransferContext;
import org.apache.reef.wake.EventHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.concurrent.ThreadSafe;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Writes happen in a serialized manner with {@link PipeContainer#putPipeListIfAbsent(Pair, int)}.
 * This ensures that each key is initialized exactly once, and never updated.
 *
 * Writes and reads for the same key never occur concurrently with no problem, because
 * (1) write never updates, and (2) read happens only after the write.
 *
 * Reads can happen concurrently with no problem.
 */
@ThreadSafe
public final class PipeContainer<I extends ByteTransferContext> {
  private static final Logger LOG = LoggerFactory.getLogger(PipeContainer.class.getName());
  private final ConcurrentHashMap<Pair<String, Long>, CountBasedBlockingContainer<I>> pipeMap;
  private final ConcurrentHashMap<Pair<String, Long>, EventHandler<Pair<I, Integer>>> pipeHandlerMap;
  private final ConcurrentMap<Pair<String, Long>, Queue<Pair<I, Integer>>> unHandledPipeHandlers;

  PipeContainer() {
    this.pipeMap = new ConcurrentHashMap<>();
    this.pipeHandlerMap = new ConcurrentHashMap<>();
    this.unHandledPipeHandlers = new ConcurrentHashMap<>();
  }

  /**
   * Blocks the get operation when the number of elements is smaller than expected.
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
        LOG.info("Set value: {}, {}", index, value);
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
   * @param pairKey
   * @param expected
   */
  synchronized void putPipeListIfAbsent(final Pair<String, Long> pairKey, final int expected) {
    pipeMap.putIfAbsent(pairKey, new CountBasedBlockingContainer(expected));
  }

  synchronized void putPipeHandlerIfAbsent(final Pair<String, Long> pairKey,
                                           final EventHandler<Pair<I, Integer>> handler) {
    pipeHandlerMap.putIfAbsent(pairKey, handler);
    final Queue<Pair<I, Integer>> list = unHandledPipeHandlers.get(pairKey);
    if (list != null) {
      while (!list.isEmpty()) {
        handler.onNext(list.poll());
      }
    }
  }

  /**
   * (SYNCHRONIZATION) CountBasedBlockingContainer takes care of it.
   *
   * @param pairKey
   * @param taskIndex src or dst
   * @param context byteinput/output context
   */
  synchronized void putPipe(final Pair<String, Long> pairKey, final int taskIndex, final I context) {
    //final CountBasedBlockingContainer<I> container = pipeMap.get(pairKey);
    //container.setValue(taskIndex, context);
    final EventHandler<Pair<I, Integer>> handler = pipeHandlerMap.get(pairKey);

    if (handler == null) {
      unHandledPipeHandlers.putIfAbsent(pairKey, new LinkedList<>());
      final Queue<Pair<I, Integer>> list = unHandledPipeHandlers.get(pairKey);
      list.add(Pair.of(context, taskIndex));
    } else {
      handler.onNext(Pair.of(context, taskIndex));
    }
  }

  /**
   * (SYNCHRONIZATION) CountBasedBlockingContainer takes care of it.
   *
   * @param pairKey
   * @return
   */
  List<I> getPipes(final Pair<String, Long> pairKey) {
    final CountBasedBlockingContainer<I> container = pipeMap.get(pairKey);
    return container.getValuesBlocking();
  }
}

