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

import org.apache.nemo.common.exception.UnsupportedCommPatternException;
import org.apache.nemo.common.ir.edge.RuntimeEdge;
import org.apache.nemo.common.ir.edge.StageEdge;
import org.apache.nemo.common.ir.edge.executionproperty.CommunicationPatternProperty;
import org.apache.nemo.common.partitioner.Partitioner;
import org.apache.nemo.common.punctuation.TimestampAndValue;
import org.apache.nemo.common.punctuation.Watermark;
import org.apache.nemo.runtime.executor.common.Serializer;
import org.apache.nemo.runtime.executor.common.WatermarkWithIndex;
import org.apache.nemo.runtime.executor.common.datatransfer.ByteOutputContext;
import org.apache.nemo.runtime.executor.common.datatransfer.ByteTransferContextSetupMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Flushable;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.*;
import java.util.stream.Collectors;

import static org.apache.nemo.common.TaskLoc.VM;

/**
 * Represents the output data transfer from a task.
 */
public final class PipeOutputWriter implements Flushable {
  private static final Logger LOG = LoggerFactory.getLogger(PipeOutputWriter.class.getName());

  private final int srcTaskIndex;
  private final PipeManagerWorker pipeManagerWorker;

  private final Partitioner partitioner;
  private final RuntimeEdge runtimeEdge;

  private boolean initialized;
  private final Serializer serializer;
  private final List<ByteOutputContext> pipes;
  private final Map<ByteOutputContext, ByteOutputContext.ByteOutputStream> pipeAndStreamMap;
  private final StageEdge stageEdge;
  private final int originTaskIndex;

  private volatile boolean closed = false;

  PipeOutputWriter(final int srcTaskIndex,
                   final int originTaskIndex,
                   final RuntimeEdge runtimeEdge,
                   final PipeManagerWorker pipeManagerWorker,
                   final Map<String, Serializer> serializerMap) {
    this.stageEdge = (StageEdge) runtimeEdge;
    this.initialized = false;
    this.originTaskIndex = originTaskIndex;
    this.srcTaskIndex = srcTaskIndex;
    this.pipeManagerWorker = pipeManagerWorker;
    //this.pipeManagerWorker.notifyMaster(runtimeEdge.getId(), RuntimeIdManager.getIndexFromTaskId(srcTaskId));
    this.partitioner = Partitioner
      .getPartitioner(stageEdge.getExecutionProperties(), stageEdge.getDstIRVertex().getExecutionProperties());
    this.runtimeEdge = runtimeEdge;
    this.pipeAndStreamMap = new HashMap<>();
    this.serializer = serializerMap.get(runtimeEdge.getId());
    this.pipes = doInitialize();
  }

  private void writeData(final Object element,
                         final List<ByteOutputContext> pipeList, final boolean flush) {
    pipeList.forEach(pipe -> {
      final ByteOutputContext.ByteOutputStream stream = pipeAndStreamMap.get(pipe);
      stream.writeElement(element, serializer, runtimeEdge.getId(), stageEdge.getDstIRVertex().getId());
      /*
      if (flush) {
        try {
          stream.flush();
        } catch (IOException e) {
          e.printStackTrace();
          throw new RuntimeException(e);
        }
      }
      */
    });
  }

  public RuntimeEdge getEdge() {
    return runtimeEdge;
  }

  public void write(final Object element) {
    final TimestampAndValue tis = (TimestampAndValue) element;
    writeData(tis, getPipeToWrite(tis), false);
  }

  public void writeWatermark(final Watermark watermark) {
    //LOG.info("Watermark emit from taskIndex {}/{}, {}", srcTaskIndex, stageEdge.getSrcIRVertex().getId(), watermark);
    final WatermarkWithIndex watermarkWithIndex = new WatermarkWithIndex(watermark, srcTaskIndex);
    // flush data whenever receiving watermarks
    writeData(watermarkWithIndex, pipes, true);
  }

  public Future<Integer> close(final String taskId) {
    // send stop message!
    closed =  true;

    final CountDownLatch count = new CountDownLatch(pipes.size());

    for (final ByteOutputContext byteOutputContext : pipes) {
      final ByteTransferContextSetupMessage pendingMsg =
        new ByteTransferContextSetupMessage(byteOutputContext.getContextId().getInitiatorExecutorId(),
          byteOutputContext.getContextId().getTransferIndex(),
          byteOutputContext.getContextId().getDataDirection(),
          byteOutputContext.getContextDescriptor(),
          byteOutputContext.getContextId().isPipe(),
          ByteTransferContextSetupMessage.MessageType.SIGNAL_FROM_PARENT_STOPPING_OUTPUT,
          VM,
          taskId);

      LOG.info("Send finish message at {} {}", taskId, pendingMsg);

      byteOutputContext.sendMessage(pendingMsg, (m) -> {
        count.countDown();
        LOG.info("receive ack from downstream at {}, {}!!", taskId, count.getCount());
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
        return false;
      }

      @Override
      public Integer get() throws InterruptedException, ExecutionException {
        try {
           //LOG.info("Waiting for ack {}...", taskId);
           count.await();
           //LOG.info("End of Waiting for ack {}...", taskId);
         } catch (InterruptedException e) {
           e.printStackTrace();
         }
        return 1;
      }

      @Override
      public Integer get(long timeout, TimeUnit unit) throws InterruptedException, ExecutionException, TimeoutException {
        return null;
      }
    };


    /*
    pipes.forEach(pipe -> {
      try {
        pipeAndStreamMap.get(pipe).close();
        pipe.close();
      } catch (IOException e) {
        throw new RuntimeException(e);
      } catch (Exception e) {
        e.printStackTrace();
        throw new RuntimeException(e);
      }
    });
    */
  }

  private List<ByteOutputContext> doInitialize() {
    LOG.info("Start - doInitialize() {}", runtimeEdge);
    initialized = true;

    final Optional<CommunicationPatternProperty.Value> comValue =
      runtimeEdge.getPropertyValue(CommunicationPatternProperty.class);

    final List<CompletableFuture<ByteOutputContext>> byteOutputContexts;
    if (comValue.get().equals(CommunicationPatternProperty.Value.OneToOne)) {
      byteOutputContexts = Collections.singletonList(
        pipeManagerWorker.write(srcTaskIndex, runtimeEdge, originTaskIndex));
      //LOG.info("Writing data: edge: {}, Task {}, Dest {}", runtimeEdge.getId(), srcTaskIndex, originTaskIndex);
    } else if (comValue.get().equals(CommunicationPatternProperty.Value.BroadCast)
      || comValue.get().equals(CommunicationPatternProperty.Value.Shuffle)) {

      final List<Integer> dstIndices = stageEdge.getDst().getTaskIndices();
      byteOutputContexts =
        dstIndices.stream()
          .map(dstTaskIndex ->
            pipeManagerWorker.write(srcTaskIndex, runtimeEdge, dstTaskIndex))
          .collect(Collectors.toList());
      //LOG.info("Writing data: edge: {}, Task {}, Dest {}", runtimeEdge.getId(), srcTaskIndex, dstIndices);
    } else {
      throw new UnsupportedCommPatternException(new Exception("Communication pattern not supported"));
    }


    final List<ByteOutputContext> boc = new ArrayList<>(byteOutputContexts.size());

    while (!byteOutputContexts.isEmpty()) {
      final Iterator<CompletableFuture<ByteOutputContext>> iterator = byteOutputContexts.iterator();
      while (iterator.hasNext()) {
         final CompletableFuture<ByteOutputContext> context = iterator.next();

         if (context.isDone()) {
           try {
             final ByteOutputContext c = context.get();
             pipeAndStreamMap.put(c, c.newOutputStream());
             boc.add(c);
           } catch (Exception e) {
             e.printStackTrace();
             throw new RuntimeException(e);
           }

           iterator.remove();
         }
      }

      try {
        Thread.sleep(200);
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
    }

    return boc;
  }

  private List<ByteOutputContext> getPipeToWrite(final TimestampAndValue element) {
    final CommunicationPatternProperty.Value comm =
      (CommunicationPatternProperty.Value) runtimeEdge.getPropertyValue(CommunicationPatternProperty.class).get();
    if (comm.equals(CommunicationPatternProperty.Value.OneToOne)) {
      return Collections.singletonList(pipes.get(0));
    } else if (comm.equals(CommunicationPatternProperty.Value.BroadCast)) {
      return pipes;
    } else {
      return Collections.singletonList(pipes.get((int) partitioner.partition(element.value)));
    }
  }

  @Override
  public void flush() throws IOException {
    if (!closed) {
      pipes.forEach(pipe -> {
        try {
          pipeAndStreamMap.get(pipe).flush();
        } catch (IOException e) {
          e.printStackTrace();
          throw new RuntimeException(e);
        }
      });
    }
  }
}
