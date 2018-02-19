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
package edu.snu.coral.runtime.executor.datatransfer;

import edu.snu.coral.common.ir.Pipe;
import edu.snu.coral.runtime.common.plan.RuntimeEdge;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Output Collector Implementation.
 * @param <O> output type.
 */
public final class PipeImpl<O> implements Pipe<O> {
  private static final Logger LOG = LoggerFactory.getLogger(PipeImpl.class.getName());
  private final AtomicReference<LinkedBlockingQueue<O>> outputQueue;
  private boolean isSideInput;
  private RuntimeEdge runtimeEdge;

  /**
   * Constructor of a new Pipe.
   */
  public PipeImpl() {
    this.outputQueue = new AtomicReference<>(new LinkedBlockingQueue<>());
    this.isSideInput = false;
    this.runtimeEdge = null;
  }

  @Override
  public void emit(final O output) {
    try {
      outputQueue.get().put(output);
    } catch (InterruptedException e) {
      throw new RuntimeException("Interrupted while PipeImpl#emit", e);
    }
  }

  @Override
  public void emit(final String dstVertexId, final Object output) {
    throw new UnsupportedOperationException("emit(dstVertexId, output) in PipeImpl.");
  }

  /**
   * Inter-Task data is transferred from sender-side Task's PipeImpl to receiver-side Task.
   * @return the output element that is transferred to the next Task of TaskGroup.
   */
  public O remove() {
    return outputQueue.get().remove();
  }

  public boolean isEmpty() {
    return outputQueue.get().isEmpty();
  }

  public int size() {
    return outputQueue.get().size();
  }

  public void markAsSideInput() {
    isSideInput = true;
  }

  public boolean isSideInput() {
    return isSideInput;
  }

  public void setRuntimeEdge(final RuntimeEdge edge) {
    runtimeEdge = edge;
  }
  public RuntimeEdge getRuntimeEdge() {
    return runtimeEdge;
  }

  public void setQueue(final LinkedBlockingQueue queue) {
    outputQueue.getAndSet(queue);
  }

  public LinkedBlockingQueue<O> duplicateQueue() {
    final LinkedBlockingQueue dupQueue = new LinkedBlockingQueue<>();
    outputQueue.get().iterator().forEachRemaining(dupQueue::add);
    LOG.info("outputQueue size after iterator: {}", outputQueue.get().size());
    return dupQueue;
  }

  /**
   * Collects the accumulated output and replace the output list.
   *
   * @return the list of output elements.
   */
  public List<O> collectOutputList() {
    LinkedBlockingQueue<O> currentQueue = outputQueue.getAndSet(new LinkedBlockingQueue<>());
    List<O> outputList = new ArrayList<>();
    currentQueue.iterator().forEachRemaining(outputList::add);
    return outputList;
  }
}
