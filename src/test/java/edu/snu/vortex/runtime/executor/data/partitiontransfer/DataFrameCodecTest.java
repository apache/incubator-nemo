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

import io.netty.channel.embedded.EmbeddedChannel;
import org.apache.reef.tang.Tang;
import org.apache.reef.tang.exceptions.InjectionException;
import org.junit.runner.RunWith;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.util.Map;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Tests for encoding and decoding data frames.
 */
@RunWith(PowerMockRunner.class)
@PrepareForTest(ControlMessageToPartitionStreamCodec.class)
public final class DataFrameCodecTest {
  private final DataFrameEncoder encoder;

  /**
   * @throws InjectionException if failed to get an instance of {@link DataFrameEncoder}
   */
  public DataFrameCodecTest() throws InjectionException {
    encoder = Tang.Factory.getTang().newInjector().getInstance(DataFrameEncoder.class);
  }

  /**
   * @param pullTransferIdToInputStream a map with input streams into which received data is recorded (pull-based)
   * @param pushTransferIdToInputStream a map with input streams into which received data is recorded (push-based)
   * @return a new {@link EmbeddedChannel} with decoder.
   */
  private EmbeddedChannel newInboundChannel(
      final Map<Short, PartitionInputStream> pullTransferIdToInputStream,
      final Map<Short, PartitionInputStream> pushTransferIdToInputStream) {
    final ControlMessageToPartitionStreamCodec mockStreamCodec = mock(ControlMessageToPartitionStreamCodec.class);
    when(mockStreamCodec.getTransferIdToInputStream(true)).thenReturn(pullTransferIdToInputStream);
    when(mockStreamCodec.getTransferIdToInputStream(false)).thenReturn(pushTransferIdToInputStream);
    return new EmbeddedChannel(new FrameDecoder());
  }

  /**
   * @return a new {@link EmbeddedChannel} with encoder.
   */
  private EmbeddedChannel newOutboundChannel() {
    return new EmbeddedChannel(encoder);
  }

  private static void testDataFrame
}
