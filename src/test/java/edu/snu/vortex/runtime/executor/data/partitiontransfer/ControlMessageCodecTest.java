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
import edu.snu.vortex.runtime.executor.data.HashRange;
import io.netty.channel.embedded.EmbeddedChannel;
import org.apache.reef.tang.Tang;
import org.apache.reef.tang.exceptions.InjectionException;
import org.junit.Test;

import static org.junit.Assert.assertTrue;

/**
 * Tests for encoding and decoding control frames.
 */
public final class ControlMessageCodecTest {
  private final ControlFrameEncoder encoder;

  /**
   * @throws InjectionException if failed to get an instance of {@link ControlFrameEncoder}
   */
  public ControlMessageCodecTest() throws InjectionException {
    encoder = Tang.Factory.getTang().newInjector().getInstance(ControlFrameEncoder.class);
  }

  /**
   * @return a new {@link EmbeddedChannel} with decoder.
   */
  private EmbeddedChannel newInboundChannel() {
    return new EmbeddedChannel(new FrameDecoder());
  }

  /**
   * @return a new {@link EmbeddedChannel} with encoder.
   */
  private EmbeddedChannel newOutboundChannel() {
    return new EmbeddedChannel(encoder);
  }

  /**
   * Simulates encoding and decoding a control frame and compare the equality.
   * @param inboundChannel  inbound {@link EmbeddedChannel}
   * @param outboundChannel outbound {@link EmbeddedChannel}
   * @param content the content of the frame to be encoded and transferred
   */
  private static void testControlFrame(
      final EmbeddedChannel inboundChannel,
      final EmbeddedChannel outboundChannel,
      final ControlMessage.DataTransferControlMessage content) {
    outboundChannel.writeOutbound(content);
    for (Object toTransferred = outboundChannel.readOutbound();
         toTransferred != null;
         toTransferred = outboundChannel.readOutbound()) {
      inboundChannel.writeInbound(toTransferred);
    }
    final Object decoded = inboundChannel.readInbound();
    assertTrue(content.equals(decoded));
  }

  @Test
  public void testControlFrames() {
    final EmbeddedChannel inboundChannel = newInboundChannel();
    final EmbeddedChannel outboundChannel = newOutboundChannel();

    final String[] sourceIds = {"SRC1", "SRC2"};
    final ControlMessage.PartitionStore[] stores = {
        null,
        ControlMessage.PartitionStore.LOCAL_FILE,
        ControlMessage.PartitionStore.MEMORY,
        ControlMessage.PartitionStore.REMOTE_FILE
    };
    final ControlMessage.PartitionTransferType[] types = {
        ControlMessage.PartitionTransferType.PULL,
        ControlMessage.PartitionTransferType.PUSH
    };
    final int[] transferIds = {0, 1, 2};
    final boolean[] encodePartialPartitions = {true, false};
    final String[] partitionIds = {"PARTITION1", "PARTITION2"};
    final String[] runtimeEdgeIds = {"EDGE1", "EDGE2"};
    final HashRange[] hashRanges = {HashRange.all(), HashRange.of(1, 4), HashRange.of(5, 8)};

    for (final String sourceId : sourceIds) {
      for (final ControlMessage.PartitionStore store : stores) {
        for (final ControlMessage.PartitionTransferType type : types) {
          for (final int transferId : transferIds) {
            for (final boolean encodePartialPartition : encodePartialPartitions) {
              for (final String partitionId : partitionIds) {
                for (final String runtimeEdgeId : runtimeEdgeIds) {
                  for (final HashRange hashRange : hashRanges) {
                    final ControlMessage.DataTransferControlMessage.Builder builder =
                        ControlMessage.DataTransferControlMessage.newBuilder();
                    builder
                        .setControlMessageSourceId(sourceId)
                        .setType(type)
                        .setTransferId(transferId)
                        .setEncodePartialPartition(encodePartialPartition)
                        .setPartitionId(partitionId)
                        .setRuntimeEdgeId(runtimeEdgeId);
                    if (store != null) {
                      builder.setPartitionStore(store);
                    }
                    if (!hashRange.isAll()) {
                      builder
                          .setStartRangeInclusive(hashRange.rangeStartInclusive())
                          .setEndRangeExclusive(hashRange.rangeEndExclusive());
                    }
                    testControlFrame(inboundChannel, outboundChannel, builder.build());
                  }
                }
              }
            }
          }
        }
      }
    }
  }
}
