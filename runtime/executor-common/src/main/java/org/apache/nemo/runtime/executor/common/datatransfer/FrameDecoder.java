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
package org.apache.nemo.runtime.executor.common.datatransfer;

import com.google.protobuf.InvalidProtocolBufferException;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufInputStream;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.ByteToMessageDecoder;
import org.apache.nemo.runtime.executor.common.controlmessages.TaskControlMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

/**
 * Interprets inbound byte streams to compose frames.
 *
 * <p>
 * More specifically,
 * <ul>
 *   <li>Recognizes the type of the frame, namely control or data.</li>
 *   <li>If the received bytes are a part of a control frame, waits until the full content of the frame becomes
 *   available and decode the frame to emit a control frame object.</li>
 *   <li>If the received bytes consists a data frame, supply the data to the corresponding {@link ByteInputContext}.
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
 *   | Zeros |  Stop/Restart  | DataDirectionFlag | NewSubStreamFlag | LastFrameFlag |  Boolean  | TransferIdx | Length  |       Body      |
 *   | 4 bit |     1 bit      |       1 bit       |      1 bit       |     1 bit     |  1 byte   |  4 bytes   | 4 bytes | Variable length |
 *   +-------+-------+-------------------+------------------+---------------+-------------+---------+-------...-------+
 * }
 * </pre>
 *
 */
public final class FrameDecoder extends ByteToMessageDecoder {
  private static final Logger LOG = LoggerFactory.getLogger(FrameDecoder.class.getName());
  private static final int HEADER_LENGTH = 2 + Integer.BYTES + Integer.BYTES;

  private final PipeManagerWorker pipeManagerWorker;

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
  private boolean broadcast = false;

  /**
   * Whether or not the data frame currently being read is the last frame of a data message.
   */
  private boolean isLastFrame;
  private boolean isStop;


  private int pipeIndex;
  private List<Integer> currPipeIndices;

  public FrameDecoder(final PipeManagerWorker pipeManagerWorker) {
    this.pipeManagerWorker = pipeManagerWorker;
  }

  @Override
  protected void decode(final ChannelHandlerContext ctx, final ByteBuf in, final List out)
      throws InvalidProtocolBufferException {
    while (true) {
      final boolean toContinue;
      if (controlBodyBytesToRead > 0) {
        toContinue = onControlBodyAdded(in, out);
      } else if (dataBodyBytesToRead > 0) {
        toContinue = onDataBodyAdded(in);
        // toContinue = in.readableBytes() > 0;
      } else if (headerRemain > 0) {
        toContinue = onBroadcastRead(ctx, in);
      } else {
        toContinue = onFrameStarted(ctx, in);
      }
      if (!toContinue) {
        break;
      }
    }
  }

  private ByteTransferContextSetupMessage.ByteTransferDataDirection dataDirection;
  private DataFrameEncoder.DataType dataType;
  private int broadcastSize;
  private byte flags;
  private long headerRemain = 0;

  private boolean onBroadcastRead(final ChannelHandlerContext ctx, final ByteBuf in) {
    // LOG.info("IsContextBroadcast size {}!!", broadcastSize);
    if (in.readableBytes() < Integer.BYTES * broadcastSize + Integer.BYTES) {
      headerRemain = Integer.BYTES * broadcastSize + Integer.BYTES;
      return false;
    }

    broadcast = true;

    currPipeIndices = new ArrayList<>(broadcastSize);

    for (int i = 0; i < broadcastSize; i++) {
      currPipeIndices.add(in.readInt());
    }

    // LOG.info("IsContextBroadcast transfier ids {}!!", currTaskIndices);

    final long length = in.readUnsignedInt();

    // setup context for reading data frame body
    dataBodyBytesToRead = length;

    final boolean newSubStreamFlag = (flags & ((byte) (1 << 1))) != 0;
    isLastFrame = (flags & ((byte) (1 << 0))) != 0;
    isStop = (flags & ((byte) (1 << 4))) != 0;

    headerRemain = 0;

    if (dataBodyBytesToRead == 0) {
      onDataFrameEnd();
    }
    return true;
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

    if (in.readableBytes() < HEADER_LENGTH) {
      // cannot read a frame header frame now
      return false;
    }

    flags = in.readByte();

    if ((flags & ((byte) (1 << 3))) == 0) {
      // setup context for reading control frame body
      // rm zero byte
      in.readByte();
      in.readInt();
      final long length = in.readUnsignedInt();
      // LOG.info("Control message...?? length {}", length);
      controlBodyBytesToRead = length;
      if (length < 0) {
        throw new IllegalStateException(String.format("Frame length is negative: %d", length));
      }

    } else {
      dataType = DataFrameEncoder.DataType.values()[(int) in.readByte()];
      final int sizeOrIndex = in.readInt();

      dataDirection =
        (flags & ((byte) (1 << 2))) == 0
          ? ByteTransferContextSetupMessage.ByteTransferDataDirection.INITIATOR_SENDS_DATA :
          ByteTransferContextSetupMessage.ByteTransferDataDirection.INITIATOR_RECEIVES_DATA;

      switch (dataType) {
        case OFFLOAD_OUTPUT:
        case NORMAL:
          broadcastSize = 0;
          broadcast = false;

          pipeIndex = sizeOrIndex;
          final long length = in.readUnsignedInt();

          // LOG.info("Receive srcTaskIndex {}->{}, body size: {}", srcTaskIndex, dtTaskIndex, length);

          // setup context for reading data frame body
          dataBodyBytesToRead = length;

          final boolean newSubStreamFlag = (flags & ((byte) (1 << 1))) != 0;
          isLastFrame = (flags & ((byte) (1 << 0))) != 0;
          isStop = (flags & ((byte) (1 << 4))) != 0;

          if (dataBodyBytesToRead == 0) {
            onDataFrameEnd();
          }
          break;
        case BROADCAST: {
          broadcastSize = sizeOrIndex;
          return onBroadcastRead(ctx, in);
        }
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

    assert (controlBodyBytesToRead <= Integer.MAX_VALUE);

    if (in.readableBytes() < controlBodyBytesToRead) {
      // cannot read body now
      return false;
    }

    final ByteBufInputStream bis = new ByteBufInputStream(in);
    final TaskControlMessage taskControlMessage = TaskControlMessage.decode(bis);

    if (taskControlMessage.type.equals(TaskControlMessage.TaskControlMessageType.OFFLOAD_CONTROL)) {
      // For offloaded task
      out.add(taskControlMessage);
    } else {
      pipeManagerWorker.addControlData(taskControlMessage.inputPipeIndex, taskControlMessage);
    }

    controlBodyBytesToRead = 0;
    return true;
  }

  // private List<ByteBuf> dataByteBufs = new LinkedList<>();

  /**
   * Supply byte stream to an existing {@link ByteInputContext}.
   *
   * @param in  the {@link ByteBuf} from which to read data
   * @throws InterruptedException when interrupted while adding to {@link ByteBuf} queue
   */
  private boolean onDataBodyAdded(final ByteBuf in) {
    assert (controlBodyBytesToRead == 0);
    assert (dataBodyBytesToRead > 0);

    if (dataBodyBytesToRead == 0) {
      throw new RuntimeException("Data body bytes zero");
    }

    if (in.readableBytes() < dataBodyBytesToRead) {
      // LOG.warn("Bytes to read smaller than dataBodyBytesToRead: "
      //  + in.readableBytes() + ", " + dataBodyBytesToRead);
      return false;
    }

    // length should not exceed Integer.MAX_VALUE (since in.readableBytes() returns an int)
    final long length = Math.min(dataBodyBytesToRead, in.readableBytes());
    assert (length <= Integer.MAX_VALUE);

    // final ByteBuf body = in.readSlice((int) length).retain();
    final ByteBuf buf = in.readRetainedSlice((int) length);

    // dataByteBufs.add(buf);
    dataBodyBytesToRead -= length;

    if (dataBodyBytesToRead != 0) {
      throw new RuntimeException(("DataBodyByesToRead should be zero"));
    }

    switch (dataType) {
      case BROADCAST: {
        for (int i = 0; i < currPipeIndices.size(); i++) {
          final Integer ti = currPipeIndices.get(i);
          pipeManagerWorker.addInputData(ti, buf.retainedDuplicate());
        }

        buf.release();
        break;
      }
      case NORMAL: {
        pipeManagerWorker.addInputData(pipeIndex, buf);
        break;
      }
      case OFFLOAD_OUTPUT: {
        // received from offloaded task
        // we redirect this event to origin executor
        pipeManagerWorker.writeData(pipeIndex, buf);
        break;
      }
    }

    onDataFrameEnd();

    return in.readableBytes() > 0;
  }

  /**
   * Closes {@link ByteInputContext} if necessary and resets the internal states of the decoder.
   */
  private void onDataFrameEnd() {
    // DO NOTHING
    /*
    if (isLastFrame) {
      inputContext.onContextClose();
    }
    if (isStop) {
      LOG.info("Context stop {}", inputContext.getContextId());
      inputContext.onContextStop();
    }
    inputContext = null;
    */
  }
}
