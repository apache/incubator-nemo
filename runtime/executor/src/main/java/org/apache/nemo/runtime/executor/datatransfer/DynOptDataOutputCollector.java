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

import org.apache.nemo.common.ir.OutputCollector;
import org.apache.nemo.common.ir.vertex.IRVertex;
import org.apache.nemo.common.ir.vertex.transform.Watermark;
import org.apache.nemo.runtime.common.RuntimeIdManager;
import org.apache.nemo.runtime.common.comm.ControlMessage;
import org.apache.nemo.runtime.common.message.MessageEnvironment;
import org.apache.nemo.runtime.common.message.PersistentConnectionToMasterMap;
import org.apache.nemo.runtime.executor.task.TaskExecutor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * OutputCollector for dynamic optimization data.
 *
 * @param <O> output type.
 */
public final class DynOptDataOutputCollector<O> implements OutputCollector<O> {
  private static final Logger LOG = LoggerFactory.getLogger(DynOptDataOutputCollector.class.getName());
  private static final String NULL_KEY = "NULL";

  private final IRVertex irVertex;
  private final PersistentConnectionToMasterMap connectionToMasterMap;
  private final TaskExecutor taskExecutor;

  public DynOptDataOutputCollector(final IRVertex irVertex,
                                   final PersistentConnectionToMasterMap connectionToMasterMap,
                                   final TaskExecutor taskExecutor) {
    this.irVertex = irVertex;
    this.connectionToMasterMap = connectionToMasterMap;
    this.taskExecutor = taskExecutor;
  }

  @Override
  public void emit(final O output) {
    final Map<Object, Long> aggregatedDynOptData = (Map<Object, Long>) output;
    final List<ControlMessage.PartitionSizeEntry> partitionSizeEntries = new ArrayList<>();
    aggregatedDynOptData.forEach((key, size) ->
      partitionSizeEntries.add(
        ControlMessage.PartitionSizeEntry.newBuilder()
          .setKey(key == null ? NULL_KEY : String.valueOf(key))
          .setSize(size)
          .build())
    );

    connectionToMasterMap.getMessageSender(MessageEnvironment.RUNTIME_MASTER_MESSAGE_LISTENER_ID)
      .send(ControlMessage.Message.newBuilder()
        .setId(RuntimeIdManager.generateMessageId())
        .setListenerId(MessageEnvironment.RUNTIME_MASTER_MESSAGE_LISTENER_ID)
        .setType(ControlMessage.MessageType.DataSizeMetric)
        .setDataSizeMetricMsg(ControlMessage.DataSizeMetricMsg.newBuilder()
          .addAllPartitionSize(partitionSizeEntries)
        )
        .build());

    // set the id of this vertex to mark the corresponding stage as put on hold
    taskExecutor.setIRVertexPutOnHold(irVertex);
  }

  @Override
  public void emitWatermark(Watermark watermark) {
    // do nothing
  }

  @Override
  public <T> void emit(final String dstVertexId, final T output) {
    throw new IllegalStateException("Dynamic optimization does not emit tagged data");
  }
}
