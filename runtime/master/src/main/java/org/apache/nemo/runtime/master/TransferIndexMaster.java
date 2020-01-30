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
package org.apache.nemo.runtime.master;

import com.google.protobuf.ByteString;
import org.apache.nemo.common.TransferKey;
import org.apache.nemo.common.exception.IllegalMessageException;
import org.apache.nemo.common.RuntimeIdManager;
import org.apache.nemo.runtime.common.comm.ControlMessage;
import org.apache.nemo.runtime.common.message.MessageContext;
import org.apache.nemo.runtime.common.message.MessageEnvironment;
import org.apache.nemo.runtime.common.message.MessageListener;
import org.apache.reef.annotations.audience.DriverSide;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.concurrent.ThreadSafe;
import javax.inject.Inject;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

import static org.apache.nemo.runtime.common.comm.ControlMessage.MessageType.RegisterTransferIndex;

/**
 * Master-side pipe manager.
 */
@ThreadSafe
@DriverSide
public final class TransferIndexMaster {
  private static final Logger LOG = LoggerFactory.getLogger(TransferIndexMaster.class.getName());

  private final AtomicInteger contextIndex;
  //private final AtomicInteger outputContextIndex;

  public final Map<TransferKey, Integer> transferIndexMap;
  public final Map<String, byte[]> serializerMap;

  /**
   * Constructor.
   * @param masterMessageEnvironment the message environment.
   */
  @Inject
  private TransferIndexMaster(final MessageEnvironment masterMessageEnvironment) {
    masterMessageEnvironment.setupListener(MessageEnvironment.TRANSFER_INDEX_LISTENER_ID,
      new TransferIndexReceiver());

    this.contextIndex = new AtomicInteger();
    this.transferIndexMap = new ConcurrentHashMap<>();
    this.serializerMap = new ConcurrentHashMap<>();
    //this.outputContextIndex = new AtomicInteger();
  }

  /**
   * Handler for control messages received.
   */
  public final class TransferIndexReceiver implements MessageListener<ControlMessage.Message> {
    @Override
    public void onMessage(final ControlMessage.Message message) {
      switch (message.getType()) {
        case RegisterTransferIndex: {
          final ControlMessage.RegisterTransferIndexMessage m = message.getRegisterTransferIndexMsg();
          final ControlMessage.TransferKeyProto keyProto = m.getKey();
          LOG.info("Registering key and index {}/{}", keyProto, m.getIndex());
          transferIndexMap.put(new TransferKey(keyProto.getEdgeId(),
            keyProto.getSrcTaskIndex(), keyProto.getDstTaskIndex(),
            keyProto.getIsOutputTransfer()), m.getIndex());
          break;
        }
        case RegisterSerializerIndex: {
          final ControlMessage.RegisterSerializerMessage m = message.getRegisterSerializerMsg();
          final String edgeId = m.getRuntimeEdgeId();
          final ByteString v = m.getSerializer();
          LOG.info("Registering serializer for {}", edgeId);
          serializerMap.put(edgeId, v.toByteArray());
          break;
        }
      }
    }

    @Override
    public void onMessageWithContext(final ControlMessage.Message message, final MessageContext messageContext) {
      switch (message.getType()) {
        case RequestTransferIndex:
          final ControlMessage.RequestTransferIndexMessage requestIndexMessage = message.getRequestTransferIndexMsg();
          final int isInputContext = (int) requestIndexMessage.getIsInputContext();

          //final int index = isInputContext == 1 ? inputContextIndex.getAndIncrement() : outputContextIndex.getAndIncrement();
          final int index = contextIndex.getAndIncrement();

          //LOG.info("Send input/output ({}) context index {}", isInputContext, index);

          messageContext.reply(
            ControlMessage.Message.newBuilder()
              .setId(RuntimeIdManager.generateMessageId())
              .setListenerId(MessageEnvironment.TRANSFER_INDEX_LISTENER_ID)
              .setType(ControlMessage.MessageType.TransferIndexInfo)
              .setTransferIndexInfoMsg(ControlMessage.TransferIndexInfoMessage.newBuilder()
                .setRequestId(message.getId())
                .setIndex(index)
                .build())
              .build());

          break;

        case LookupTransferIndex: {
          final ControlMessage.LookupTransferIndexMesssage m = message.getLookupTransferIndexMsg();
          final ControlMessage.TransferKeyProto key = m.getKey();

          if (!transferIndexMap.containsKey(key)) {
            throw new RuntimeException("No transfer key registered " + key);
          }

          final int keyIndex = transferIndexMap.get(key);

          messageContext.reply(
            ControlMessage.Message.newBuilder()
              .setId(RuntimeIdManager.generateMessageId())
              .setListenerId(MessageEnvironment.TRANSFER_INDEX_LISTENER_ID)
              .setType(ControlMessage.MessageType.ReturnTransferIndex)
              .setReturnTransferIndexMsg(ControlMessage.ReturnTransferIndexMessage
                .newBuilder()
                .setIndex(keyIndex)
                .build())
              .build());
          break;
        }

        /*
        case LookupSerializerIndex: {
          final ControlMessage.LookupSerializerMesssage m = message.getLookupSerializerMsg();
          final String key = m.getRuntimeEdgeId();

          if (!serializerMap.containsKey(key)) {
            throw new RuntimeException("No serializer key registered " + key);
          }

          messageContext.reply(
            ControlMessage.Message.newBuilder()
              .setId(RuntimeIdManager.generateMessageId())
              .setListenerId(MessageEnvironment.TRANSFER_INDEX_LISTENER_ID)
              .setType(ControlMessage.MessageType.ReturnSerializerIndex)
              .setReturnSerializerMsg(ControlMessage.ReturnSerializerMessage
                .newBuilder()
                .setSerializer(serializerMap.get(key))
                .build())
              .build());
          break;
        }
        */
        default:
          throw new IllegalMessageException(new Exception(message.toString()));
      }
    }
  }
}
