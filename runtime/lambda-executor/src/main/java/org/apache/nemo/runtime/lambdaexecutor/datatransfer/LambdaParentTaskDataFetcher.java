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
package org.apache.nemo.runtime.lambdaexecutor.datatransfer;

import org.apache.nemo.common.Pair;
import org.apache.nemo.common.RuntimeIdManager;
import org.apache.nemo.common.Util;
import org.apache.nemo.common.ir.AbstractOutputCollector;
import org.apache.nemo.common.ir.OutputCollector;
import org.apache.nemo.common.ir.edge.RuntimeEdge;
import org.apache.nemo.common.ir.vertex.IRVertex;
import org.apache.nemo.common.ir.vertex.SourceVertex;
import org.apache.nemo.common.punctuation.EmptyElement;
import org.apache.nemo.common.punctuation.Watermark;
import org.apache.nemo.runtime.executor.common.*;
import org.apache.nemo.runtime.executor.common.datatransfer.InputReader;
import org.apache.nemo.runtime.executor.common.datatransfer.IteratorWithNumBytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.concurrent.NotThreadSafe;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Optional;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

/**
 * Task thread -> fetchDataElement() -> (((QUEUE))) <- List of iterators <- queueInsertionThreads
 *
 * Unlike, where the task thread directly consumes (and blocks on) iterators one by one,
 * this class spawns threads that each forwards elements from an iterator to a global queue.
 *
 * This class should be used when dealing with unbounded data streams, as we do not want to be blocked on a
 * single unbounded iterator forever.
 */
@NotThreadSafe
public final class LambdaParentTaskDataFetcher extends DataFetcher {
  private static final Logger LOG = LoggerFactory.getLogger(LambdaParentTaskDataFetcher.class);

  private final InputReader readersForParentTask;
  //private final ExecutorService queueInsertionThreads;

  // Non-finals (lazy fetching)
  private boolean firstFetch = true;

  private final ConcurrentLinkedQueue<Watermark> watermarkQueue;

  private long serBytes = 0;
  private long encodedBytes = 0;

  private int numOfIterators; // == numOfIncomingEdges
  private int numOfFinishMarks = 0;

  // A watermark manager
  private InputWatermarkManager inputWatermarkManager;

  private final String taskId;

  private final List<IteratorWithNumBytes> iterators;

  private int iteratorIndex = 0;

  private final ConcurrentMap<IteratorWithNumBytes, Integer> iteratorTaskIndexMap;
  private final ConcurrentMap<Integer, IteratorWithNumBytes> taskIndexIteratorMap;

  private final ExecutorService queueInsertionThreads;
  private final ConcurrentLinkedQueue elementQueue;

  private final ConcurrentLinkedQueue<Pair<IteratorWithNumBytes, Integer>> taskAddPairQueue;
  private volatile boolean watermarkTriggered = false;

  private long prevWatermarkTimestamp = -1L;

  private final ScheduledExecutorService watermarkTrigger;

  private final RendevousServerClient rendevousServerClient;

  private final String stageId;

  private static final long WATERMARK_PROGRESS = Util.WATERMARK_PROGRESS;

  private final AtomicBoolean stopped = new AtomicBoolean(false);

  private final AtomicBoolean prepared;

  private final ExecutorService prepareService;

  public LambdaParentTaskDataFetcher(final String taskId,
                                     final IRVertex dataSource,
                                     final RuntimeEdge edge,
                                     final InputReader readerForParentTask,
                                     final OutputCollector outputCollector,
                                     final RendevousServerClient rendevousServerClient,
                                     final TaskExecutor taskExecutor,
                                     final AtomicBoolean prepared,
                                     final ExecutorService prepareService) {
    super(dataSource, edge, outputCollector);

    readerForParentTask.setDataFetcher(this);
    this.prepared = prepared;
    this.prepareService = prepareService;

    this.taskId = taskId;
    this.stageId = RuntimeIdManager.getStageIdFromTaskId(taskId);
    this.readersForParentTask = readerForParentTask;
    this.firstFetch = true;
    this.watermarkQueue = new ConcurrentLinkedQueue<>();
    this.elementQueue = new ConcurrentLinkedQueue();

    //this.queueInsertionThreads = Executors.newCachedThreadPool();
    this.iterators = new ArrayList<>();
    this.iteratorTaskIndexMap = new ConcurrentHashMap<>();
    this.taskIndexIteratorMap = new ConcurrentHashMap<>();
    this.queueInsertionThreads = Executors.newCachedThreadPool();
    this.taskAddPairQueue = new ConcurrentLinkedQueue<>();

    this.rendevousServerClient = rendevousServerClient;

    this.watermarkTrigger = Executors.newSingleThreadScheduledExecutor();
    /*
    watermarkTrigger.scheduleAtFixedRate(() -> {

      synchronized (stopped) {
        if (!stopped.get() && prepared.get()) {
          final Optional<Long> watermark = rendevousServerClient.requestWatermark(taskId);
          //LOG.info("Request watermark at {}", taskId);

          if (watermark.isPresent() && prevWatermarkTimestamp + Util.WATERMARK_PROGRESS <= watermark.get()) {
            //LOG.info("Receive watermark at {}: {}", taskId, new Instant(watermark.get()));
            prevWatermarkTimestamp = watermark.get();
            taskExecutor.handleIntermediateWatermarkEvent(new Watermark(watermark.get()), this);
          }
        }
      }
    }, 3000, 200, TimeUnit.MILLISECONDS);
    */
  }

  @Override
  public void prepare() {
    LOG.info("Prepare data fetcher {}", taskId);
    this.iterators.addAll(fetchBlocking());
    LOG.info("End of Prepare data fetcher {}", taskId);
  }

  @Override
  public boolean isAvailable() {
    for (final IteratorWithNumBytes iterator : iterators) {
      if (iterator.hasNext()) {
        return true;
      }
    }

    return watermarkTriggered;
  }

  @Override
  public boolean hasData() {
    for (final IteratorWithNumBytes iterator : iterators) {
      if (iterator.hasNext()) {
        return true;
      }
    }
    return false;
  }

  @Override
  public Object fetchDataElement() throws IOException, NoSuchElementException {
    if (firstFetch) {
      //fetchDataLazily();
      firstFetch = false;
    }

    // Emit watermark
    if (watermarkTriggered) {
      watermarkTriggered = false;
      // index=0 as there is only 1 input stream
      final Optional<Long> watermark = rendevousServerClient.requestWatermark(taskId);

      if (watermark.isPresent() && prevWatermarkTimestamp + WATERMARK_PROGRESS <= watermark.get()) {
        prevWatermarkTimestamp = watermark.get();
        return new Watermark(watermark.get());
      }
    }


    int cnt = 0;
    while (cnt < iterators.size()) {
      final IteratorWithNumBytes iterator = iterators.get(iteratorIndex);

      if (iterator.isFinished()) {
        // TODO: nothing!
        throw new RuntimeException("Iterator should be finished");

      } else if (iterator.hasNext()) {
        iteratorIndex = (iteratorIndex + 1) % iterators.size();
        final Object element = iterator.next();
        // data element
        return element;
      } else {
        //LOG.info("No next element in iterator {}, cnt: {}, size: {}", iteratorIndex, cnt, iterators.size());
        iteratorIndex = (iteratorIndex + 1) % iterators.size();
        cnt += 1;
      }
    }

    return EmptyElement.getInstance();
  }

  @Override
  public Future<Integer> stop(final String taskId) {
    synchronized (stopped) {
      stopped.set(true);
    }
    watermarkTrigger.shutdown();
    try {
      watermarkTrigger.awaitTermination(2, TimeUnit.SECONDS);
    } catch (InterruptedException e) {
      e.printStackTrace();
    }
    return readersForParentTask.stop(taskId);
  }

  @Override
  public void restart() {
    readersForParentTask.restart();
  }

  private List<IteratorWithNumBytes> fetchBlocking() {
    // 갯수 동적으로 받아야함. handler 같은거 등록하기
    final List<IteratorWithNumBytes> inputs = readersForParentTask.read()
      .stream().map(future -> {
        try {
          return future.get();
        } catch (InterruptedException e) {
          e.printStackTrace();
          throw new RuntimeException(e);
        } catch (ExecutionException e) {
          e.printStackTrace();
          throw new RuntimeException(e);
        }
      })
      .collect(Collectors.toList());

    numOfIterators = inputs.size();

    LOG.info("Number of parents: {}", numOfIterators);

    if (numOfIterators > 1) {
      inputWatermarkManager = new MultiInputWatermarkManager(getDataSource(), numOfIterators, new WatermarkCollector(), taskId);
    } else {
      inputWatermarkManager = new SingleInputWatermarkManager(
        new WatermarkCollector(), null, null, null, null);
    }

    return inputs;
  }

  @Override
  public void close() throws Exception {
    queueInsertionThreads.shutdown();
  }

  /**
   * Just adds the emitted watermark to the element queue.
   * It receives the watermark from InputWatermarkManager.
   */
  private final class WatermarkCollector extends AbstractOutputCollector {
    @Override
    public void emit(final Object output) {
      throw new IllegalStateException("Should not be called");
    }
    @Override
    public void emitWatermark(final Watermark watermark) {
      watermarkQueue.offer(watermark);
    }
    @Override
    public void emit(final String dstVertexId, final Object output) {
      throw new IllegalStateException("Should not be called");
    }
  }
}
