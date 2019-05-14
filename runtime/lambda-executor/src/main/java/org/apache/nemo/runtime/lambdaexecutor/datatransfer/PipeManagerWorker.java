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
import org.apache.nemo.common.ir.edge.RuntimeEdge;
import org.apache.nemo.common.ir.edge.StageEdge;
import org.apache.nemo.common.ir.edge.executionproperty.CommunicationPatternProperty;
import org.apache.nemo.common.ir.vertex.executionproperty.ParallelismProperty;

import org.apache.nemo.runtime.executor.common.Serializer;
import org.apache.nemo.runtime.executor.common.datatransfer.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.concurrent.ThreadSafe;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Two threads use this class
 * - Network thread: Saves pipe connections created from destination tasks.
 * - Task executor thread: Creates new pipe connections to destination tasks (read),
 *                         or retrieves a saved pipe connection (write)
 */
@ThreadSafe
public final class PipeManagerWorker {
  private static final Logger LOG = LoggerFactory.getLogger(PipeManagerWorker.class.getName());

  private final String executorId;
  private final Map<Pair<String, Integer>, String> taskExecutorIdMap;

  // To-Executor connections
  private final ByteTransfer byteTransfer;

  private final Map<String, Serializer> serializerMap;

  private final Set<ByteInputContext> byteInputContexts = new HashSet<>();

  public PipeManagerWorker(final String executorId,
                           final ByteTransfer byteTransfer,
                           final Map<Pair<String, Integer>, String> taskExecutorIdMap,
                           final Map<String, Serializer> serializerMap) {
    this.executorId = executorId;
    this.byteTransfer = byteTransfer;
    this.taskExecutorIdMap = taskExecutorIdMap;
    this.serializerMap = serializerMap;
  }


  private boolean isExecutorInSF(final String targetExecutorId) {
    // TODO..
    return false;
  }

  public Future<Integer> stop() {
    final AtomicInteger atomicInteger = new AtomicInteger(byteInputContexts.size());

    for (final ByteInputContext byteInputContext : byteInputContexts) {
      final ByteTransferContextSetupMessage pendingMsg =
        new ByteTransferContextSetupMessage(executorId,
          byteInputContext.getContextId().getTransferIndex(),
          byteInputContext.getContextId().getDataDirection(),
          byteInputContext.getContextDescriptor(),
          byteInputContext.getContextId().isPipe(),
          ByteTransferContextSetupMessage.MessageType.PENDING_FOR_SCALEIN_VM);

      LOG.info("Send message {}", pendingMsg);

      byteInputContext.sendMessage(pendingMsg, (m) -> {

        LOG.info("receive ack!!");
        final int d = atomicInteger.decrementAndGet();

        if (d == 0) {
          // close conneciton
        }

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

  public CompletableFuture<ByteOutputContext> write(final int srcTaskIndex,
                                                    final RuntimeEdge runtimeEdge,
                                                    final int dstTaskIndex) {
    final String runtimeEdgeId = runtimeEdge.getId();
    // TODO: check whether it is in SF or not
    final String targetExecutorId = taskExecutorIdMap.get(Pair.of(runtimeEdge.getId(), dstTaskIndex));

    if (isExecutorInSF(targetExecutorId)) {
      // Connect to the relay server!
      // TODO
      throw new UnsupportedOperationException("Not supported yet");
    } else {
      // The executor is in VM, just connects to the VM server
      // Descriptor
      final PipeTransferContextDescriptor descriptor =
        new PipeTransferContextDescriptor(
          runtimeEdgeId,
          srcTaskIndex,
          dstTaskIndex,
          getNumOfInputPipeToWait(runtimeEdge));

      LOG.info("Writer descriptor: runtimeEdgeId: {}, srcTaskIndex: {}, dstTaskIndex: {}, getNumOfInputPipe:{} ",
        runtimeEdgeId, srcTaskIndex, dstTaskIndex, getNumOfInputPipeToWait(runtimeEdge));
      // Connect to the executor
      return byteTransfer.newOutputContext(targetExecutorId, descriptor.encode(), true)
        .thenApply(context -> context);
    }
  }

  public CompletableFuture<IteratorWithNumBytes> read(final int srcTaskIndex,
                                                      final RuntimeEdge runtimeEdge,
                                                      final int dstTaskIndex) {
    final String runtimeEdgeId = runtimeEdge.getId();
    final String srcExecutorId = taskExecutorIdMap.get(Pair.of(runtimeEdge.getId(), srcTaskIndex));

    if (isExecutorInSF(srcExecutorId)) {
       // Connect to the relay server!
      // TODO
      throw new UnsupportedOperationException("Not supported yet");
    } else {
      // Descriptor
      final PipeTransferContextDescriptor descriptor =
        new PipeTransferContextDescriptor(
          runtimeEdgeId,
          srcTaskIndex,
          dstTaskIndex,
          getNumOfOutputPipeToWait(runtimeEdge));

      // Connect to the executor
      return byteTransfer.newInputContext(srcExecutorId, descriptor.encode(), true)
        .thenApply(context -> {
          byteInputContexts.add(context);
          return ((StreamRemoteByteInputContext) context).getInputIterator(
            serializerMap.get(runtimeEdgeId));
        });
    }
  }

  private int getNumOfOutputPipeToWait(final RuntimeEdge runtimeEdge) {
    final int srcParallelism = ((StageEdge) runtimeEdge).getDst().getPropertyValue(ParallelismProperty.class)
      .orElseThrow(() -> new IllegalStateException());
    final CommunicationPatternProperty.Value commPattern = ((StageEdge) runtimeEdge)
      .getPropertyValue(CommunicationPatternProperty.class)
      .orElseThrow(() -> new IllegalStateException());
    return commPattern.equals(CommunicationPatternProperty.Value.OneToOne) ? 1 : srcParallelism;
  }

  private int getNumOfInputPipeToWait(final RuntimeEdge runtimeEdge) {
    final int srcParallelism = ((StageEdge) runtimeEdge).getSrc().getPropertyValue(ParallelismProperty.class)
      .orElseThrow(() -> new IllegalStateException());
    final CommunicationPatternProperty.Value commPattern = ((StageEdge) runtimeEdge)
      .getPropertyValue(CommunicationPatternProperty.class)
      .orElseThrow(() -> new IllegalStateException());
    return commPattern.equals(CommunicationPatternProperty.Value.OneToOne) ? 1 : srcParallelism;
  }
}
