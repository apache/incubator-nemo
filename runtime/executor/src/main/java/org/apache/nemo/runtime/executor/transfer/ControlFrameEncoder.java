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
package org.apache.nemo.runtime.executor.transfer;

import com.google.protobuf.ByteString;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToMessageEncoder;
import org.apache.nemo.conf.JobConf;
import org.apache.nemo.runtime.common.comm.ControlMessage.ByteTransferContextSetupMessage;
import org.apache.reef.tang.annotations.Parameter;

import javax.inject.Inject;
import java.util.List;

/**
 * Encodes a control frame into bytes.
 *
 * @see FrameDecoder
 */
@ChannelHandler.Sharable
final class ControlFrameEncoder extends MessageToMessageEncoder<ByteTransferContext> {

  private static final int ZEROS_LENGTH = 5;
  private static final int BODY_LENGTH_LENGTH = Integer.BYTES;
  private static final ByteBuf ZEROS = Unpooled.directBuffer(ZEROS_LENGTH, ZEROS_LENGTH).writeZero(ZEROS_LENGTH);

  private final String localExecutorId;

  /**
   * Private constructor.
   */
  @Inject
  private ControlFrameEncoder(@Parameter(JobConf.ExecutorId.class) final String localExecutorId) {
    this.localExecutorId = localExecutorId;
  }

  @Override
  protected void encode(final ChannelHandlerContext ctx,
                        final ByteTransferContext in,
                        final List out) {
    final ByteTransferContextSetupMessage message = ByteTransferContextSetupMessage.newBuilder()
      .setInitiatorExecutorId(localExecutorId)
      .setTransferIndex(in.getContextId().getTransferIndex())
      .setDataDirection(in.getContextId().getDataDirection())
      .setContextDescriptor(ByteString.copyFrom(in.getContextDescriptor()))
      .setIsPipe(in.getContextId().isPipe())
      .build();
    final byte[] frameBody = message.toByteArray();
    out.add(ZEROS.retain());
    out.add(ctx.alloc().ioBuffer(BODY_LENGTH_LENGTH, BODY_LENGTH_LENGTH).writeInt(frameBody.length));
    out.add(Unpooled.wrappedBuffer(frameBody));
  }
}
