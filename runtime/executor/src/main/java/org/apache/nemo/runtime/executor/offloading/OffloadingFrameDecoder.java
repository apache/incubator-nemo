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
package org.apache.nemo.runtime.executor.offloading;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufInputStream;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToMessageDecoder;
import org.apache.commons.lang3.tuple.Triple;
import org.apache.nemo.offloading.common.OffloadingExecutorControlEvent;
import org.apache.nemo.offloading.common.Pair;
import org.apache.nemo.runtime.executor.PipeIndexMapWorker;
import org.apache.nemo.runtime.executor.TaskExecutorMapWrapper;
import org.apache.nemo.runtime.executor.common.ExecutorThread;
import org.apache.nemo.runtime.executor.common.TaskOffloadedDataOutputEvent;
import org.apache.nemo.runtime.executor.common.datatransfer.DataFrameEncoder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import java.util.*;

@ChannelHandler.Sharable
public final class OffloadingFrameDecoder extends MessageToMessageDecoder<ByteBuf> {
  private static final Logger LOG = LoggerFactory.getLogger(OffloadingFrameDecoder.class.getName());

  private final TaskExecutorMapWrapper taskExecutorMapWrapper;
  private final PipeIndexMapWorker pipeIndexMapWorker;
  private final OffloadingDataTransportEventHandler nemoEventHandler;

  @Inject
  private OffloadingFrameDecoder(final TaskExecutorMapWrapper taskExecutorMapWrapper,
                                 final PipeIndexMapWorker pipeIndexMapWorker,
                                 final OffloadingDataTransportEventHandler nemoEventHandler) {
    this.taskExecutorMapWrapper = taskExecutorMapWrapper;
    this.pipeIndexMapWorker = pipeIndexMapWorker;
    this.nemoEventHandler = nemoEventHandler;
  }

  @Override
  protected void decode(ChannelHandlerContext ctx, ByteBuf msg, List<Object> out) throws Exception {
    final int controlOrData = msg.readByte();

    if (controlOrData == 1) {
      // data
      decodeData(ctx, msg, out);
    } else {
      // control
      decodeControl(ctx, msg, out);
    }
  }

  private void decodeControl(ChannelHandlerContext ctx, ByteBuf msg, List<Object> out) {
    final OffloadingExecutorControlEvent.Type type =
      OffloadingExecutorControlEvent.Type.values()[msg.readByte()];

    nemoEventHandler.onNext(Pair.of(ctx.channel(), new OffloadingExecutorControlEvent(type, msg)));
  }

  private void decodeData(ChannelHandlerContext ctx, ByteBuf msg, List<Object> out) throws Exception {
    final DataFrameEncoder.DataType dataType = DataFrameEncoder.DataType.values()[msg.readByte()];
    msg.retain();

    switch (dataType) {
      case OFFLOAD_BROADCAST_OUTPUT: {
        try {
          final ByteBufInputStream dis = new ByteBufInputStream(msg);
          final int len = dis.readInt();
          final List<Integer> indices = new ArrayList<>(len);
          for (int i = 0; i < len; i++) {
            indices.add(dis.readInt());
          }
          final List<String> dstTasks = new ArrayList<String>(len);
          for (final int index : indices) {
            final Triple<String, String, String> key = pipeIndexMapWorker.getKey(index);
            dstTasks.add(key.getRight());
          }

          // LOG.info("Offload broadcast SRC {} edge {} dst: {}", srcTask, edge, dstTasks);
          final Triple<String, String, String> key = pipeIndexMapWorker.getKey(indices.get(0));
          final ExecutorThread et = taskExecutorMapWrapper.getTaskExecutorThread(key.getLeft());
          et.addEvent(new TaskOffloadedDataOutputEvent(key.getLeft(), key.getMiddle(), dstTasks, msg));
        } catch (final Exception e) {
          e.printStackTrace();
          throw new RuntimeException(e);
        }
        break;
      }
      case OFFLOAD_NORMAL_OUTPUT: {
        try {
          final ByteBufInputStream dis = new ByteBufInputStream(msg);
          final int index = dis.readInt();
          final Triple<String, String, String> key = pipeIndexMapWorker.getKey(index);

          // LOG.info("Offload normal SRC {} edge {} dst: {}", srcTask, edge, dstTask);
          final ExecutorThread et = taskExecutorMapWrapper.getTaskExecutorThread(key.getLeft());
          et.addEvent(new TaskOffloadedDataOutputEvent(
            key.getLeft(),
            key.getMiddle(),
            Collections.singletonList(key.getRight()), msg));
        } catch (final Exception e) {
          e.printStackTrace();
          throw new RuntimeException(e);
        }
        break;
      }
      case DEOFFLOAD_DONE: {
        final ByteBufInputStream dis = new ByteBufInputStream(msg);
        final String taskId = dis.readUTF();
        final ExecutorThread executorThread = taskExecutorMapWrapper.getTaskExecutorThread(taskId);

        LOG.info("Receive deoffloading done {}", taskId);
        msg.release();

        /* TODO: enable
        executorThread.addEvent(new TaskOffloadingEvent(taskId,
          TaskOffloadingEvent.ControlType.DEOFFLOADING_DONE,
          null));
          */
        break;
      }
      default: {
        throw new RuntimeException("Not supported datatype " + dataType);
      }
    }
  }

}
