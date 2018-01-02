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
package edu.snu.onyx.runtime.executor.data.blocktransfer;

import javax.annotation.concurrent.ThreadSafe;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;

/**
 * A blocking {@link Iterable} implementation which is capable of closing the input end.
 *
 * @param <T> the type of elements
 */
@ThreadSafe
public final class ClosableBlockingIterable<T> implements Iterable<T>, AutoCloseable {

  private final List<T> list;
  private volatile boolean closed = false;

  /**
   * Creates a closable blocking iterable.
   */
  public ClosableBlockingIterable() {
    list = new ArrayList<>();
  }

  /**
   * Creates a closable blocking iterable.
   *
   * @param numElements the initial capacity
   */
  public ClosableBlockingIterable(final int numElements) {
    list = new ArrayList<>(numElements);
  }

  /**
   * Adds an element to the end of the underlying list.
   *
   * @param element the element to add
   * @throws IllegalStateException if this iterable is closed
   */
  public synchronized void add(final T element) {
    if (closed) {
      throw new IllegalStateException("This iterable has been closed");
    }
    list.add(element);
    notifyAll();
  }

  /**
   * Mark the input end of this queue as closed.
   */
  public synchronized void close() {
    closed = true;
    notifyAll();
  }

  @Override
  public Iterator<T> iterator() {
    return new ClosableBlockingIterator<>(this);
  }

  /**
   * {@link Iterator} for {@link ClosableBlockingIterable}.
   *
   * @param <T> the type of elements
   */
  private static final class ClosableBlockingIterator<T> implements Iterator<T> {

    private ClosableBlockingIterable<T> iterable;

    /**
     * Creates a {@link ClosableBlockingIterator}.
     *
     * @param iterable the corresponding {@link ClosableBlockingIterable}
     */
    private ClosableBlockingIterator(final ClosableBlockingIterable<T> iterable) {
      this.iterable = iterable;
    }

    private int index = 0;

    /**
     * {@inheritDoc}
     *
     * @throws RuntimeException if interrupted while waiting for the element
     */
    @Override
    public boolean hasNext() {
      try {
        synchronized (iterable) {
          while (iterable.list.size() <= index && !iterable.closed) {
            iterable.wait();
          }
          return iterable.list.size() > index;
        }
      } catch (final InterruptedException e) {
        throw new RuntimeException(e);
      }
    }

    /**
     * {@inheritDoc}
     *
     * @throws RuntimeException if interrupted while waiting for the element
     */
    @Override
    public T next() {
      try {
        synchronized (iterable) {
          while (iterable.list.size() <= index && !iterable.closed) {
            iterable.wait();
          }
          final T element = iterable.list.get(index);
          index++;
          if (element == null) {
            throw new NoSuchElementException();
          } else {
            return element;
          }
        }
      } catch (final InterruptedException e) {
        throw new RuntimeException(e);
      }
    }
  }
}
