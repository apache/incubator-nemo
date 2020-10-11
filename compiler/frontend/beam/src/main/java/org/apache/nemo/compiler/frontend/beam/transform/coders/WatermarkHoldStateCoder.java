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
package org.apache.nemo.compiler.frontend.beam.transform.coders;

import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.CoderException;
import org.apache.beam.sdk.state.WatermarkHoldState;
import org.apache.beam.sdk.transforms.windowing.TimestampCombiner;
import org.apache.nemo.common.coder.FSTSingleton;
import org.apache.nemo.compiler.frontend.beam.transform.InMemoryStateInternals;
import org.joda.time.Instant;
import org.nustaq.serialization.FSTConfiguration;

import java.io.*;
import java.util.Collections;
import java.util.List;

/**
 * Coder for {@link WatermarkHoldState}.
 */
public final class WatermarkHoldStateCoder extends Coder<WatermarkHoldState> {


  private WatermarkHoldStateCoder() {
  }

  private static final WatermarkHoldStateCoder INSTANCE = new WatermarkHoldStateCoder();

  public static WatermarkHoldStateCoder getInstance() {
    return INSTANCE;
  }

  @Override
  public void encode(final WatermarkHoldState value, final OutputStream outStream) throws CoderException, IOException {
    final FSTConfiguration conf = FSTSingleton.getInstance();
    final DataOutputStream dos = new DataOutputStream(outStream);

    final Instant instant = value.read();

    if (instant == null) {
      outStream.write(1);
    } else {
      outStream.write(0);

      dos.writeLong(instant.getMillis());
    }

    final TimestampCombiner timestampCombiner = value.getTimestampCombiner();
    conf.encodeToStream(outStream, timestampCombiner);
  }

  @Override
  public WatermarkHoldState decode(final InputStream inStream) throws CoderException, IOException {
    final FSTConfiguration conf = FSTSingleton.getInstance();
    final DataInputStream dis = new DataInputStream(inStream);

    final int b = inStream.read();

    final Instant instant;

    try {
      if (b == 0) {
        instant = new Instant(dis.readLong());
      } else {
        instant = null;
      }

      final TimestampCombiner timestampCombiner = (TimestampCombiner) conf.decodeFromStream(inStream);
      final WatermarkHoldState watermarkHoldState =
        new InMemoryStateInternals.InMemoryWatermarkHold<>(timestampCombiner);

      if (instant != null) {
        watermarkHoldState.add(instant);
      }
      return watermarkHoldState;
    } catch (final Exception e) {
      e.printStackTrace();
      throw new RuntimeException(e);
    }
  }

  @Override
  public List<? extends Coder<?>> getCoderArguments() {
    return Collections.EMPTY_LIST;
  }

  @Override
  public void verifyDeterministic() throws NonDeterministicException {
  }
}
