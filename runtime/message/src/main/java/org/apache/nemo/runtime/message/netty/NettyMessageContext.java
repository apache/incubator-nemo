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

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufOutputStream;
import io.netty.channel.Channel;
import org.apache.nemo.runtime.common.comm.ControlMessage;
import org.apache.nemo.runtime.message.MessageContext;
import org.apache.reef.exception.evaluator.NetworkException;
import org.apache.reef.io.network.Connection;
import org.apache.reef.io.network.ConnectionFactory;
import org.apache.reef.wake.IdentifierFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * Message context for NCS.
 */
final class NettyMessageContext implements MessageContext {
  private static final Logger LOG = LoggerFactory.getLogger(NettyMessageContext.class.getName());

  private final int messageId;
  private final Channel channel;

  NettyMessageContext(final int messageId,
                      final Channel channel) {
    this.messageId = messageId;
    this.channel = channel;
  }

  @Override
  @SuppressWarnings("squid:S2095")
  public <U> void reply(final U replyMessage) {

    final ControlMessage.Message message = (ControlMessage.Message) replyMessage;

    final ByteBuf byteBuf = channel.alloc().ioBuffer();
    final ByteBufOutputStream bos = new ByteBufOutputStream(byteBuf);
    try {
      message.writeTo(bos);
      bos.close();
    } catch (IOException e) {
      e.printStackTrace();
    }
    channel.writeAndFlush(byteBuf);
  }
}
