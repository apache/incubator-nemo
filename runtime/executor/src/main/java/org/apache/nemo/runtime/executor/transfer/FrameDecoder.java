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

import com.google.protobuf.InvalidProtocolBufferException;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.ByteToMessageDecoder;
import org.apache.nemo.runtime.common.comm.ControlMessage.ByteTransferContextSetupMessage;
import org.apache.nemo.runtime.common.comm.ControlMessage.ByteTransferDataDirection;

import java.util.List;

/**
 * Interprets inbound byte streams to compose frames.
 * <p>
 * <p>
 * More specifically,
 * <ul>
 * <li>Recognizes the type of the frame, namely control or data.</li>
 * <li>If the received bytes are a part of a control frame, waits until the full content of the frame becomes
 * available and decode the frame to emit a control frame object.</li>
 * <li>If the received bytes consists a data frame, supply the data to the corresponding {@link ByteInputContext}.
 * </ul>
 *
 * <h3>Control frame specification:</h3>
 * <pre>
 * {@literal
 *   <----- HEADER ----> <----- BODY ----->
 *   +--------+---------+-------...-------+
 *   |  Zeros | Length  |       Body      |
 *   | 5 byte | 4 bytes | Variable length |
 *   +--------+---------+-------...-------+
 * }
 * </pre>
 *
 * <h3>Data frame specification:</h3>
 * <pre>
 * {@literal
 *   <---------------------------------- HEADER ---------------------------------------------------> <----- BODY ----->
 *   +-------+-------+-------------------+------------------+---------------+-------------+---------+-------...-------+
 *   | Zeros |   1   | DataDirectionFlag | NewSubStreamFlag | LastFrameFlag | TransferIdx | Length  |       Body      |
 *   | 4 bit | 1 bit |       1 bit       |      1 bit       |     1 bit     |   4 bytes   | 4 bytes | Variable length |
 *   +-------+-------+-------------------+------------------+---------------+-------------+---------+-------...-------+
 * }
 * </pre>
 *
 * @see ByteTransportChannelInitializer
 */
final class FrameDecoder extends ByteToMessageDecoder {

  private static final int HEADER_LENGTH = 9;

  private final ContextManager contextManager;

  /**
   * The number of bytes consisting body of a control frame to be read next.
   */
  private long controlBodyBytesToRead = 0;

  /**
   * The number of bytes consisting body of a data frame to be read next.
   */
  private long dataBodyBytesToRead = 0;

  /**
   * The {@link ByteInputContext} to which received bytes are added.
   */
  private ByteInputContext inputContext;

  /**
   * Whether or not the data frame currently being read is the last frame of a data message.
   */
  private boolean isLastFrame;

  FrameDecoder(final ContextManager contextManager) {
    this.contextManager = contextManager;
  }

  @Override
  protected void decode(final ChannelHandlerContext ctx, final ByteBuf in, final List out)
    throws InvalidProtocolBufferException {
    while (true) {
      final boolean toContinue;
      if (controlBodyBytesToRead > 0) {
        toContinue = onControlBodyAdded(in, out);
      } else if (dataBodyBytesToRead > 0) {
        onDataBodyAdded(in);
        toContinue = in.readableBytes() > 0;
      } else {
        toContinue = onFrameStarted(ctx, in);
      }
      if (!toContinue) {
        break;
      }
    }
  }

  /**
   * Try to decode a frame header.
   *
   * @param ctx the channel handler context
   * @param in  the {@link ByteBuf} from which to read data
   * @return {@code true} if a header was decoded, {@code false} otherwise
   */
  private boolean onFrameStarted(final ChannelHandlerContext ctx, final ByteBuf in) {
    assert (controlBodyBytesToRead == 0);
    assert (dataBodyBytesToRead == 0);
    assert (inputContext == null);

    if (in.readableBytes() < HEADER_LENGTH) {
      // cannot read a frame header frame now
      return false;
    }
    final byte flags = in.readByte();
    final int transferIndex = in.readInt();
    final long length = in.readUnsignedInt();
    if (length < 0) {
      throw new IllegalStateException(String.format("Frame length is negative: %d", length));
    }
    if ((flags & ((byte) (1 << 3))) == 0) {
      // setup context for reading control frame body
      controlBodyBytesToRead = length;
    } else {
      // setup context for reading data frame body
      dataBodyBytesToRead = length;
      final ByteTransferDataDirection dataDirection = (flags & ((byte) (1 << 2))) == 0
        ? ByteTransferDataDirection.INITIATOR_SENDS_DATA : ByteTransferDataDirection.INITIATOR_RECEIVES_DATA;
      final boolean newSubStreamFlag = (flags & ((byte) (1 << 1))) != 0;
      isLastFrame = (flags & ((byte) (1 << 0))) != 0;
      inputContext = contextManager.getInputContext(dataDirection, transferIndex);
      if (inputContext == null) {
        throw new IllegalStateException(String.format("Transport context for %s:%d was not found between the local"
            + "address %s and the remote address %s", dataDirection, transferIndex,
          ctx.channel().localAddress(), ctx.channel().remoteAddress()));
      }
      if (newSubStreamFlag) {
        inputContext.onNewStream();
      }
      if (dataBodyBytesToRead == 0) {
        onDataFrameEnd();
      }
    }
    return true;
  }

  /**
   * Try to emit the body of the control frame.
   *
   * @param in  the {@link ByteBuf} from which to read data
   * @param out the list to which the body of the control frame is added
   * @return {@code true} if the control frame body was emitted, {@code false} otherwise
   * @throws InvalidProtocolBufferException when failed to parse
   */
  private boolean onControlBodyAdded(final ByteBuf in, final List out)
    throws InvalidProtocolBufferException {
    assert (controlBodyBytesToRead > 0);
    assert (dataBodyBytesToRead == 0);
    assert (inputContext == null);

    assert (controlBodyBytesToRead <= Integer.MAX_VALUE);

    if (in.readableBytes() < controlBodyBytesToRead) {
      // cannot read body now
      return false;
    }

    final byte[] bytes;
    final int offset;
    if (in.hasArray()) {
      bytes = in.array();
      offset = in.arrayOffset() + in.readerIndex();
    } else {
      bytes = new byte[(int) controlBodyBytesToRead];
      in.getBytes(in.readerIndex(), bytes, 0, (int) controlBodyBytesToRead);
      offset = 0;
    }
    final ByteTransferContextSetupMessage controlMessage
      = ByteTransferContextSetupMessage.PARSER.parseFrom(bytes, offset, (int) controlBodyBytesToRead);

    out.add(controlMessage);
    in.skipBytes((int) controlBodyBytesToRead);
    controlBodyBytesToRead = 0;
    return true;
  }

  /**
   * Supply byte stream to an existing {@link ByteInputContext}.
   *
   * @param in the {@link ByteBuf} from which to read data
   * @throws InterruptedException when interrupted while adding to {@link ByteBuf} queue
   */
  private void onDataBodyAdded(final ByteBuf in) {
    assert (controlBodyBytesToRead == 0);
    assert (dataBodyBytesToRead > 0);
    assert (inputContext != null);

    // length should not exceed Integer.MAX_VALUE (since in.readableBytes() returns an int)
    final long length = Math.min(dataBodyBytesToRead, in.readableBytes());
    assert (length <= Integer.MAX_VALUE);
    final ByteBuf body = in.readSlice((int) length).retain();
    inputContext.onByteBuf(body);

    dataBodyBytesToRead -= length;
    if (dataBodyBytesToRead == 0) {
      onDataFrameEnd();
    }
  }

  /**
   * Closes {@link ByteInputContext} if necessary and resets the internal states of the decoder.
   */
  private void onDataFrameEnd() {
    if (isLastFrame) {
      inputContext.onContextClose();
    }
    inputContext = null;
  }
}
