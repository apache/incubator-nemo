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

import com.google.protobuf.InvalidProtocolBufferException;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufInputStream;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.ByteToMessageDecoder;
import io.netty.handler.codec.MessageToMessageDecoder;
import io.netty.handler.codec.MessageToMessageEncoder;
import org.apache.nemo.runtime.executor.TaskExecutorMapWrapper;
import org.apache.nemo.runtime.executor.common.ExecutorThread;
import org.apache.nemo.runtime.executor.common.TaskOffloadedDataOutputEvent;
import org.apache.nemo.runtime.executor.common.datatransfer.DataFrameEncoder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import java.util.*;

public final class OffloadingFrameDecoder extends MessageToMessageDecoder<ByteBuf> {
  private static final Logger LOG = LoggerFactory.getLogger(OffloadingFrameDecoder.class.getName());

  private final TaskExecutorMapWrapper taskExecutorMapWrapper;

  @Inject
  private OffloadingFrameDecoder(final TaskExecutorMapWrapper taskExecutorMapWrapper) {
    this.taskExecutorMapWrapper = taskExecutorMapWrapper;
  }

  @Override
  protected void decode(ChannelHandlerContext ctx, ByteBuf msg, List<Object> out) throws Exception {
    final DataFrameEncoder.DataType dataType = DataFrameEncoder.DataType.values()[msg.readByte()];

    msg.retain();

    switch (dataType) {
      case OFFLOAD_BROADCAST_OUTPUT: {
        try {
          final ByteBufInputStream dis = new ByteBufInputStream(msg);
          final String srcTask = dis.readUTF();
          final String edge = dis.readUTF();
          final int dstSize = dis.readInt();
          final List<String> dstTasks = new ArrayList<String>(dstSize);
          for (int i = 0; i < dstSize; i++) {
            dstTasks.add(dis.readUTF());
          }

          // LOG.info("Offload broadcast SRC {} edge {} dst: {}", srcTask, edge, dstTasks);

          final ExecutorThread et = taskExecutorMapWrapper.getTaskExecutorThread(srcTask);
          et.addEvent(new TaskOffloadedDataOutputEvent(srcTask, edge, dstTasks, msg));
        } catch (final Exception e) {
          e.printStackTrace();
          throw new RuntimeException(e);
        }
        break;
      }
      case OFFLOAD_NORMAL_OUTPUT: {
        try {
          final ByteBufInputStream dis = new ByteBufInputStream(msg);
          final String srcTask = dis.readUTF();
          final String edge = dis.readUTF();
          final String dstTask = dis.readUTF();

          // LOG.info("Offload normal SRC {} edge {} dst: {}", srcTask, edge, dstTask);

          final ExecutorThread et = taskExecutorMapWrapper.getTaskExecutorThread(srcTask);
          et.addEvent(new TaskOffloadedDataOutputEvent(srcTask, edge, Collections.singletonList(dstTask), msg));
        } catch (final Exception e) {
          e.printStackTrace();
          throw new RuntimeException(e);
        }
        break;
      }
      default: {
        throw new RuntimeException("Not supported datatype " + dataType);
      }
    }
  }
}
