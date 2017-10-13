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
package edu.snu.vortex.runtime.executor.data.partitiontransfer;

import edu.snu.vortex.runtime.common.comm.ControlMessage;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.embedded.EmbeddedChannel;
import org.apache.commons.lang3.RandomUtils;
import org.apache.reef.tang.Tang;
import org.apache.reef.tang.exceptions.InjectionException;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.util.*;

import static org.mockito.Matchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import static org.junit.Assert.assertTrue;

/**
 * Tests for encoding and decoding data frames.
 */
@RunWith(PowerMockRunner.class)
@PrepareForTest({ControlMessageToPartitionStreamCodec.class, PartitionInputStream.class})
public final class DataFrameCodecTest {
  private static final int NUM_FRAMES = 5;
  private static final int LENGTH_START_INCLUSIVE = 30;
  private static final int LENGTH_END_EXCLUSIVE = 300;
  private final DataFrameEncoder encoder;

  /**
   * @throws InjectionException if failed to get an instance of {@link DataFrameEncoder}
   */
  public DataFrameCodecTest() throws InjectionException {
    encoder = Tang.Factory.getTang().newInjector().getInstance(DataFrameEncoder.class);
  }

  /**
   * @param mockStreamCodecWrapper wrapper for mock {@link ControlMessageToPartitionStreamCodec}.
   * @return a new {@link EmbeddedChannel} with decoder.
   */
  private static EmbeddedChannel newInboundChannel(final MockStreamCodecWrapper mockStreamCodecWrapper) {
    return new EmbeddedChannel(new FrameDecoder(), mockStreamCodecWrapper.getMock());
  }

  /**
   * @return a new {@link EmbeddedChannel} with encoder.
   */
  private EmbeddedChannel newOutboundChannel() {
    return new EmbeddedChannel(encoder);
  }

  /**
   * Simulates data frame transfer and checks the equality.
   * @param inboundChannel          inbound embedded channel
   * @param outboundChannel         outbound embedded channel
   * @param mockInputStreamWrapper  wrapper for mock {@link MockInputStreamWrapper}
   * @param isPull      whether the transfer is pull-based or not
   * @param transferId  the transfer id
   * @param data        the data to transfer
   */
  private static void testDataFrameTransfer(
      final EmbeddedChannel inboundChannel,
      final EmbeddedChannel outboundChannel,
      final MockInputStreamWrapper mockInputStreamWrapper,
      final boolean isPull,
      final short transferId,
      final byte[] data) {
    final ControlMessage.PartitionTransferType type = isPull ? ControlMessage.PartitionTransferType.PULL
        : ControlMessage.PartitionTransferType.PUSH;
    final DataFrameEncoder.DataFrame dataFrame = DataFrameEncoder.DataFrame.newInstance(type, false, transferId,
        data.length, Unpooled.wrappedBuffer(data));
    outboundChannel.writeOutbound(dataFrame);
    for (Object toTransferred = outboundChannel.readOutbound();
         toTransferred != null;
         toTransferred = outboundChannel.readOutbound()) {
      inboundChannel.writeInbound(toTransferred);
    }
    final byte[] received = mockInputStreamWrapper.pollCollectedOutput();
    assertTrue(Arrays.equals(data, received));
  }

  private static byte[] generateRandomData() {
    return RandomUtils.nextBytes(RandomUtils.nextInt(LENGTH_START_INCLUSIVE, LENGTH_END_EXCLUSIVE));
  }

  @Test
  public void testDataFrameTransfers() {
    final MockStreamCodecWrapper mockStreamCodecWrapper = new MockStreamCodecWrapper();
    final EmbeddedChannel inboundChannel = newInboundChannel(mockStreamCodecWrapper);
    final EmbeddedChannel outboundChannel = newOutboundChannel();

    // set up four transport contexts
    for (final boolean isPull : new boolean[]{true, false, false, true}) {
      final MockInputStreamWrapper mockInputStreamWrapper = mockStreamCodecWrapper.newInputStream(isPull);
      final short transferId = mockInputStreamWrapper.getTransferId();
      // for each context, send five data frames
      for (int i = 0; i < NUM_FRAMES; i++) {
        testDataFrameTransfer(inboundChannel, outboundChannel, mockInputStreamWrapper, isPull, transferId,
            generateRandomData());
      }
    }
  }

  /**
   * Wrapper for mock {@link ControlMessageToPartitionStreamCodec}.
   */
  private static final class MockStreamCodecWrapper {
    private final Map<Short, PartitionInputStream> pullTransferIdToMock = new HashMap<>();
    private final Map<Short, PartitionInputStream> pushTransferIdToMock = new HashMap<>();
    private short nextPullTransferId = 0;
    private short nextPushTransferId = 0;
    private final ControlMessageToPartitionStreamCodec mockStreamCodec;

    MockStreamCodecWrapper() {
      mockStreamCodec = mock(ControlMessageToPartitionStreamCodec.class);
      when(mockStreamCodec.getTransferIdToInputStream(true)).thenReturn(pullTransferIdToMock);
      when(mockStreamCodec.getTransferIdToInputStream(false)).thenReturn(pushTransferIdToMock);
    }

    ControlMessageToPartitionStreamCodec getMock() {
      return mockStreamCodec;
    }

    synchronized MockInputStreamWrapper newInputStream(final boolean isPull) {
      final short transferId = (isPull ? nextPullTransferId++ : nextPushTransferId++);
      final MockInputStreamWrapper wrapper = new MockInputStreamWrapper(isPull, transferId);
      (isPull ? pullTransferIdToMock : pushTransferIdToMock).put(transferId, wrapper.getMock());
      return wrapper;
    }
  }

  /**
   * Wrapper for mock {@link PartitionInputStream}.
   */
  private static final class MockInputStreamWrapper {
    private final boolean isPull;
    private final short transferId;
    private final List<ByteBuf> byteBufs = new ArrayList<>();
    private final PartitionInputStream mockStream;
    private boolean isEnded = false;

    MockInputStreamWrapper(final boolean isPull, final short transferId) {
      this.isPull = isPull;
      this.transferId = transferId;
      mockStream = mock(PartitionInputStream.class);
      doAnswer(invocation -> {
        assertTrue(!isEnded);
        byteBufs.add((ByteBuf) invocation.getArguments()[0]);
        return null;
      }).when(mockStream).append(any());
      doAnswer(invocation -> {
        assertTrue(!isEnded);
        isEnded = true;
        return null;
      }).when(mockStream).markAsEnded();
    }

    PartitionInputStream getMock() {
      return mockStream;
    }

    byte[] pollCollectedOutput() {
      int size = 0;
      for (final ByteBuf byteBuf : byteBufs) {
        size += byteBuf.readableBytes();
      }
      final byte[] output = new byte[size];
      int offset = 0;
      for (final ByteBuf byteBuf : byteBufs) {
        final int numBytes = byteBuf.readableBytes();
        byteBuf.readBytes(output, offset, numBytes);
        offset += numBytes;
      }
      byteBufs.clear();
      return output;
    }

    boolean isPull() {
      return isPull;
    }

    short getTransferId() {
      return transferId;
    }

    boolean isEnded() {
      return isEnded;
    }
  }
}
