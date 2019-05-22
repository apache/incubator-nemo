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
package org.apache.nemo.runtime.executor.datatransfer;

import org.apache.nemo.common.NemoTriple;
import org.apache.nemo.common.Pair;
import org.apache.nemo.common.exception.UnsupportedCommPatternException;
import org.apache.nemo.common.ir.edge.executionproperty.CommunicationPatternProperty;
import org.apache.nemo.common.ir.vertex.IRVertex;
import org.apache.nemo.common.ir.edge.RuntimeEdge;
import org.apache.nemo.runtime.executor.common.TaskLocationMap;
import org.apache.nemo.runtime.executor.common.datatransfer.*;
import org.apache.nemo.runtime.executor.bytetransfer.LocalByteInputContext;
import org.apache.nemo.runtime.executor.relayserver.RelayServer;
import org.apache.nemo.runtime.lambdaexecutor.datatransfer.LambdaRemoteByteInputContext;
import org.apache.nemo.runtime.executor.data.DataUtil;
import org.apache.nemo.runtime.executor.data.PipeManagerWorker;
import org.apache.reef.wake.EventHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

/**
 * Represents the input data transfer to a task.
 */
public final class PipeInputReader implements InputReader {

  private static final Logger LOG = LoggerFactory.getLogger(PipeInputReader.class.getName());

  private final PipeManagerWorker pipeManagerWorker;

  private final int dstTaskIndex;

  /**
   * Attributes that specify how we should read the input.
   */
  private final IRVertex srcVertex;
  private final RuntimeEdge runtimeEdge;
  private final TaskInputContextMap taskInputContextMap;

  private final Set<ByteInputContext> byteInputContexts;

  private final String executorId;
  private final RelayServer relayServer;
  private final TaskLocationMap taskLocationMap;

  PipeInputReader(final String executorId,
                  final int dstTaskIdx,
                  final IRVertex srcIRVertex,
                  final RuntimeEdge runtimeEdge,
                  final PipeManagerWorker pipeManagerWorker,
                  final TaskInputContextMap taskInputContextMap,
                  final RelayServer relayServer,
                  final TaskLocationMap taskLocationMap) {
    this.executorId = executorId;
    this.dstTaskIndex = dstTaskIdx;
    this.srcVertex = srcIRVertex;
    this.taskInputContextMap = taskInputContextMap;
    this.runtimeEdge = runtimeEdge;
    this.pipeManagerWorker = pipeManagerWorker;
    this.byteInputContexts = new HashSet<>();
    this.relayServer = relayServer;
    this.taskLocationMap = taskLocationMap;
    this.pipeManagerWorker.notifyMaster(runtimeEdge.getId(), dstTaskIdx);
  }


  @Override
  public Future<Integer> stop() {
    final AtomicInteger atomicInteger = new AtomicInteger(byteInputContexts.size());

    for (final ByteInputContext byteInputContext : byteInputContexts) {
      final ByteTransferContextSetupMessage pendingMsg =
        new ByteTransferContextSetupMessage(executorId,
          byteInputContext.getContextId().getTransferIndex(),
          byteInputContext.getContextId().getDataDirection(),
          byteInputContext.getContextDescriptor(),
          byteInputContext.getContextId().isPipe(),
          ByteTransferContextSetupMessage.MessageType.PENDING_FOR_SCALEOUT_VM,
          relayServer.getPublicAddress(),
          relayServer.getPort());

      LOG.info("Send message {}", pendingMsg);

      byteInputContext.sendMessage(pendingMsg, (m) -> {
        TODO: receive relay server channel address..

        LOG.info("receive ack!!");
        atomicInteger.decrementAndGet();

        //byteInputContext.sendMessage();
        //throw new RuntimeException("TODO");
      });

    }

    return new Future<Integer>() {
      @Override
      public boolean cancel(boolean mayInterruptIfRunning) {
        return false;
      }

      @Override
      public boolean isCancelled() {
        return false;
      }

      @Override
      public boolean isDone() {
        return atomicInteger.get() == 0;
      }

      @Override
      public Integer get() throws InterruptedException, ExecutionException {
        return byteInputContexts.size();
      }

      @Override
      public Integer get(long timeout, TimeUnit unit) throws InterruptedException, ExecutionException, TimeoutException {
        final long st = System.currentTimeMillis();
        while (System.currentTimeMillis() - st < unit.toMillis(timeout)) {
          if (isDone()) {
            return get();
          } else {
            Thread.sleep(200);
          }
        }

        throw new TimeoutException();
      }
    };
  }

  @Override
  public void restart() {
    final AtomicInteger atomicInteger = new AtomicInteger(byteInputContexts.size());

    for (final ByteInputContext byteInputContext : byteInputContexts) {
      final ByteTransferContextSetupMessage pendingMsg =
        new ByteTransferContextSetupMessage(executorId,
          byteInputContext.getContextId().getTransferIndex(),
          byteInputContext.getContextId().getDataDirection(),
          byteInputContext.getContextDescriptor(),
          byteInputContext.getContextId().isPipe(),
          ByteTransferContextSetupMessage.MessageType.RESUME_AFTER_SCALEIN_VM);

      LOG.info("Send resume message {}", pendingMsg);

      byteInputContext.sendMessage(pendingMsg, (m) -> {

        LOG.info("receive ack!!");
        atomicInteger.decrementAndGet();

        //byteInputContext.sendMessage();
        //throw new RuntimeException("TODO");
      });

    }
  }

  @Override
  public List<CompletableFuture<IteratorWithNumBytes>> read() {
    final Optional<CommunicationPatternProperty.Value> comValue =
      runtimeEdge.getPropertyValue(CommunicationPatternProperty.class);

    if (comValue.get().equals(CommunicationPatternProperty.Value.OneToOne)) {
      return Collections.singletonList(pipeManagerWorker.read(dstTaskIndex, runtimeEdge, dstTaskIndex));
    } else if (comValue.get().equals(CommunicationPatternProperty.Value.BroadCast)
      || comValue.get().equals(CommunicationPatternProperty.Value.Shuffle)) {
      final int numSrcTasks = InputReader.getSourceParallelism(this);
      final List<CompletableFuture<IteratorWithNumBytes>> futures = new ArrayList<>();
      for (int srcTaskIdx = 0; srcTaskIdx < numSrcTasks; srcTaskIdx++) {
        futures.add(pipeManagerWorker.read(srcTaskIdx, runtimeEdge, dstTaskIndex));
      }
      return futures;
    } else {
      throw new UnsupportedCommPatternException(new Exception("Communication pattern not supported"));
    }
  }

  @Override
  public void readAsync(final String taskId,
                        final EventHandler<Pair<IteratorWithNumBytes, Integer>> handler) {
    pipeManagerWorker
      .registerInputContextHandler(runtimeEdge, dstTaskIndex, pair -> {
        final ByteInputContext context = pair.left();
        taskInputContextMap.put(taskId, context);

        byteInputContexts.add(context);

        final int srcTaskIndex = pair.right();

        taskLocationMap.locationMap
          .put(new NemoTriple<>(runtimeEdge.getId(), srcTaskIndex, false), TaskLocationMap.LOC.VM);

        if (context instanceof LocalByteInputContext) {
          final LocalByteInputContext localByteInputContext = (LocalByteInputContext) context;
          handler.onNext(Pair.of(localByteInputContext.getIteratorWithNumBytes(), srcTaskIndex));
        } else if (context instanceof LambdaRemoteByteInputContext) {
          handler.onNext(Pair.of(new DataUtil.InputStreamIterator(context.getInputStreams(),
            pipeManagerWorker.getSerializerManager().getSerializer(runtimeEdge.getId())), srcTaskIndex));
        } else if (context instanceof StreamRemoteByteInputContext) {
          handler.onNext(Pair.of(((StreamRemoteByteInputContext) context).getInputIterator(
            pipeManagerWorker.getSerializerManager().getSerializer(runtimeEdge.getId())),
            srcTaskIndex));
        }
      });
  }

  public List<IteratorWithNumBytes> readBlocking() {

    /**********************************************************/
    /* 여기서 pipe container 같은거 사용하기
    /**********************************************************/
    final List<ByteInputContext> byteInputContexts = pipeManagerWorker.getInputContexts(runtimeEdge, dstTaskIndex);

    return byteInputContexts.stream()
      .map(context -> {
        return new DataUtil.InputStreamIterator(context.getInputStreams(),
          pipeManagerWorker.getSerializerManager().getSerializer(runtimeEdge.getId()));
      })
      .collect(Collectors.toList());


    /**********************************************************/
    /**********************************************************/
  }

  @Override
  public IRVertex getSrcIrVertex() {
    return srcVertex;
  }

  @Override
  public int getTaskIndex() {
    return dstTaskIndex;
  }

}
