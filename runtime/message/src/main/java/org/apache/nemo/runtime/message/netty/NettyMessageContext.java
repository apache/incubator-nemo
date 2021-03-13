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
package org.apache.nemo.runtime.message.netty;

import io.netty.channel.Channel;
import org.apache.nemo.runtime.common.comm.ControlMessage;
import org.apache.nemo.runtime.message.AbstractMessageContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * Message context for NCS.
 */
final class NettyMessageContext extends AbstractMessageContext {
  private static final Logger LOG = LoggerFactory.getLogger(NettyMessageContext.class.getName());

  private final String myId;
  private final Channel channel;

  NettyMessageContext(final String myId,
                      final long requestId,
                      final Channel channel) {
    super(requestId);
    this.myId = myId;
    this.channel = channel;
  }

  @Override
  @SuppressWarnings("squid:S2095")
  public <U> void reply(final U replyMessage) {
    final ControlMessage.Message message = (ControlMessage.Message) replyMessage;
    // LOG.info("Reply message from {} {}/{}/{} to {}", myId, message.getId(), message.getType(),
    //  message.getListenerId(), channel.remoteAddress());
    synchronized (channel) {
      channel.writeAndFlush(new ControlMessageWrapper(ControlMessageWrapper.MsgType.Reply, message));
    }

    /*
    .addListener(new GenericFutureListener<Future<? super Void>>() {
      @Override
      public void operationComplete(Future<? super Void> future) throws Exception {
        LOG.info("Completed reply message {}/{}/{} to {}", message.getId(), message.getType(),
          message.getListenerId(),
          channel.remoteAddress());
      }
    });
    */
  }
}
