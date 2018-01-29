/*
 * Copyright (C) 2017 Seoul National University
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package edu.snu.onyx.runtime.executor.data.blocktransfer;

import com.google.protobuf.InvalidProtocolBufferException;
import edu.snu.onyx.runtime.common.comm.ControlMessage;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.ByteToMessageDecoder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;

/**
 * Interprets inbound byte streams to compose frames.
 *
 * <p>
 * More specifically,
 * <ul>
 *   <li>Recognizes the type of the frame, namely control or data.</li>
 *   <li>If the received bytes are a part of a control frame, waits until the full content of the frame becomes
 *   available and decode the frame to emit a control frame object.</li>
 *   <li>If the received bytes consists a data frame, supply the data to the corresponding {@link BlockInputStream}.
 *   <li>If the end of a data message is recognized, closes the corresponding {@link BlockInputStream}.</li>
 * </ul>
 *
 * <h3>Control frame specification:</h3>
 * <pre>
 * {@literal
 *   <------------ HEADER -----------> <----- BODY ----->
 *   +---------+------------+---------+-------...-------+
 *   |   Type  |   Unused   |  Length |       Body      |
 *   | 2 bytes |  2 bytes   | 4 bytes | Variable length |
 *   +---------+------------+---------+-------...-------+
 * }
 * </pre>
 *
 * <h3>Data frame specification:</h3>
 * <pre>
 * {@literal
 *   <------------ HEADER -----------> <----- BODY ----->
 *   +---------+------------+---------+-------...-------+
 *   |   Type  | TransferId |  Length |       Body      |
 *   | 2 bytes |  2 bytes   | 4 bytes | Variable length |
 *   +---------+------------+---------+-------...-------+
 * }
 * </pre>
 *
 * <h3>Literals used in frame header:</h3>
 * <ul>
 *   <li>Type
 *     <ul>
 *       <li>0: control frame</li>
 *       <li>2: data frame for pull-based transfer</li>
 *       <li>3: data frame for pull-based transfer, the last frame of the message</li>
 *       <li>4: data frame for push-based transfer</li>
 *       <li>5: data frame for push-based transfer, the last frame of the message</li>
 *     </ul>
 *   </li>
 *   <li>TransferId: the transfer id to distinguish which transfer this frame belongs to</li>
 *   <li>Length: the number of bytes in the body, not the entire frame</li>
 * </ul>
 *
 * @see BlockTransportChannelInitializer
 */
final class FrameDecoder extends ByteToMessageDecoder {

  private static final Logger LOG = LoggerFactory.getLogger(FrameDecoder.class);

  static final short CONTROL_TYPE = 0;
  static final short PULL_INTERMEDIATE_FRAME = 2;
  static final short PULL_LASTFRAME = 3;
  static final short PUSH_INTERMEDIATE_FRAME = 4;
  static final short PUSH_LASTFRAME = 5;

  static final int HEADER_LENGTH = ControlFrameEncoder.HEADER_LENGTH;

  private Map<Short, BlockInputStream> pullTransferIdToInputStream;
  private Map<Short, BlockInputStream> pushTransferIdToInputStream;

  /**
   * The number of bytes consisting body of a control frame to be read next.
   */
  private long controlBodyBytesToRead = 0;

  /**
   * The number of bytes consisting body of a data frame to be read next.
   */
  private long dataBodyBytesToRead = 0;

  /**
   * The {@link BlockInputStream} to which received bytes are added.
   */
  private BlockInputStream inputStream;

  /**
   * Whether or not the data frame currently being read is the last frame of a data message.
   */
  private boolean isLastFrame;

  /**
   * Whether or not the data frame currently being read consists a pull-based transfer.
   */
  private boolean isPullTransfer;

  /**
   * The id of transfer currently being executed.
   */
  private short transferId;

  /**
   * Creates a frame decoder.
   */
  FrameDecoder() {
    assert (ControlFrameEncoder.HEADER_LENGTH == DataFrameEncoder.HEADER_LENGTH);
  }

  @Override
  public void channelActive(final ChannelHandlerContext ctx) {
    final ControlMessageToBlockStreamCodec duplexHandler
        = ctx.channel().pipeline().get(ControlMessageToBlockStreamCodec.class);
    pullTransferIdToInputStream = duplexHandler.getPullTransferIdToInputStream();
    pushTransferIdToInputStream = duplexHandler.getPushTransferIdToInputStream();
    ctx.fireChannelActive();
  }

  @Override
  protected void decode(final ChannelHandlerContext ctx, final ByteBuf in, final List out)
      throws InvalidProtocolBufferException {
    while (true) {
      final boolean toContinue;
      if (controlBodyBytesToRead > 0) {
        toContinue = onControlBodyAdded(ctx, in, out);
      } else if (dataBodyBytesToRead > 0) {
        onDataBodyAdded(ctx, in);
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
    assert (inputStream == null);

    if (in.readableBytes() < HEADER_LENGTH) {
      // cannot read a frame header frame now
      return false;
    }
    final short type = in.readShort();
    transferId = in.readShort();
    final long length = in.readUnsignedInt();
    if (length < 0) {
      throw new IllegalStateException(String.format("Frame length is negative: %d", length));
    }
    if (type == CONTROL_TYPE) {
      // setup context for reading control frame body
      controlBodyBytesToRead = length;
    } else {
      // setup context for reading data frame body
      dataBodyBytesToRead = length;
      isPullTransfer = type == PULL_INTERMEDIATE_FRAME || type == PULL_LASTFRAME;
      final boolean isPushTransfer = type == PUSH_INTERMEDIATE_FRAME || type == PUSH_LASTFRAME;
      if (!isPullTransfer && !isPushTransfer) {
        throw new IllegalStateException(String.format("Illegal frame type: %d", type));
      }
      isLastFrame = type == PULL_LASTFRAME || type == PUSH_LASTFRAME;
      inputStream = (isPullTransfer ? pullTransferIdToInputStream : pushTransferIdToInputStream).get(transferId);
      if (inputStream == null) {
        throw new IllegalStateException(String.format("Transport context for %s:%d was not found between the local"
                + "address %s and the remote address %s", isPullTransfer ? "pull" : "push", transferId,
            ctx.channel().localAddress(), ctx.channel().remoteAddress()));
      }
      if (dataBodyBytesToRead == 0) {
        onDataFrameEnd(ctx);
      }
    }
    return true;
  }

  /**
   * Try to emit the body of the control frame.
   *
   * @param ctx the channel handler context
   * @param in  the {@link ByteBuf} from which to read data
   * @param out the list to which the body of the control frame is added
   * @return {@code true} if the control frame body was emitted, {@code false} otherwise
   * @throws InvalidProtocolBufferException when failed to parse
   */
  private boolean onControlBodyAdded(final ChannelHandlerContext ctx, final ByteBuf in, final List out)
      throws InvalidProtocolBufferException {
    assert (controlBodyBytesToRead > 0);
    assert (dataBodyBytesToRead == 0);
    assert (inputStream == null);

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
    final ControlMessage.DataTransferControlMessage controlMessage
        = ControlMessage.DataTransferControlMessage.PARSER.parseFrom(bytes, offset, (int) controlBodyBytesToRead);

    out.add(controlMessage);
    in.skipBytes((int) controlBodyBytesToRead);
    controlBodyBytesToRead = 0;
    return true;
  }

  /**
   * Supply byte stream to an existing {@link BlockInputStream}.
   *
   * @param ctx the channel handler context
   * @param in  the {@link ByteBuf} from which to read data
   * @throws InterruptedException when interrupted while adding to {@link ByteBuf} queue
   */
  private void onDataBodyAdded(final ChannelHandlerContext ctx, final ByteBuf in) {
    assert (controlBodyBytesToRead == 0);
    assert (dataBodyBytesToRead > 0);
    assert (inputStream != null);

    // length should not exceed Integer.MAX_VALUE (since in.readableBytes() returns an int)
    final long length = Math.min(dataBodyBytesToRead, in.readableBytes());
    assert (length <= Integer.MAX_VALUE);
    final ByteBuf body = in.readSlice((int) length).retain();
    inputStream.append(body);

    dataBodyBytesToRead -= length;
    if (dataBodyBytesToRead == 0) {
      onDataFrameEnd(ctx);
    }
  }

  /**
   * Closes {@link BlockInputStream} if necessary and resets the internal states of the decoder.
   *
   * @param ctx the channel handler context
   * @throws InterruptedException when interrupted while marking the end of stream
   */
  private void onDataFrameEnd(final ChannelHandlerContext ctx) {
    if (isLastFrame) {
      LOG.debug("Transport {}:{}, where the block sender is {} and the receiver is {}, is now closed",
          new Object[]{isPullTransfer ? "pull" : "push", transferId, ctx.channel().remoteAddress(),
              ctx.channel().localAddress()});
      inputStream.markAsEnded();
      (isPullTransfer ? pullTransferIdToInputStream : pushTransferIdToInputStream).remove(transferId);
    }
    inputStream = null;
  }
}
