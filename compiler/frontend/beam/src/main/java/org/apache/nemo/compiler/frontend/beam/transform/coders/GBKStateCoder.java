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
import org.apache.nemo.common.punctuation.Watermark;
import org.apache.nemo.compiler.frontend.beam.transform.GBKState;
import org.apache.nemo.compiler.frontend.beam.transform.InMemoryStateInternalsFactory;
import org.apache.nemo.compiler.frontend.beam.transform.InMemoryTimerInternalsFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Coder for {@link GBKState}.
 * @param <K> key type
 */
public final class GBKStateCoder<K> extends Coder<GBKState<K>> {
  private static final Logger LOG = LoggerFactory.getLogger(GBKStateCoder.class.getName());
  private final Coder<K> keyCoder;
  private final Coder windowCoder;
  private final InMemoryTimerInternalsFactoryCoder<K> timerCoder;
  private final InMemoryStateInternalsFactoryCoder<K> stateCoder;

  public GBKStateCoder(final Coder<K> keyCoder,
                            final Coder windowCoder) {
    this.timerCoder = new InMemoryTimerInternalsFactoryCoder<>(keyCoder, windowCoder);
    this.stateCoder = new InMemoryStateInternalsFactoryCoder<>(keyCoder, windowCoder);
    this.keyCoder = keyCoder;
    this.windowCoder = windowCoder;
  }

  @Override
  public void encode(final GBKState<K> value, final OutputStream outStream) throws CoderException, IOException {
    final DataOutputStream dos = new DataOutputStream(outStream);

    final long st = System.currentTimeMillis();
    timerCoder.encode(value.timerInternalsFactory, outStream);

    final long st1 = System.currentTimeMillis();
    stateCoder.encode(value.stateInternalsFactory, outStream);

    final long st2 = System.currentTimeMillis();

    dos.writeLong(value.prevOutputWatermark.getTimestamp());
    dos.writeLong(value.inputWatermark.getTimestamp());

    encodeKeyAndWatermarkMap(value.keyAndWatermarkHoldMap, dos);
    final long st3 = System.currentTimeMillis();

    LOG.info("Encoding time: timer: {}, state: {}, keyWatermark: {}", (st1 - st), (st2 - st1), (st3 - st2));
  }


  @Override
  public GBKState<K> decode(final InputStream inStream) throws CoderException, IOException {


    final long st = System.currentTimeMillis();
    final InMemoryTimerInternalsFactory timerInternalsFactory = timerCoder.decode(inStream);

    final long st1 = System.currentTimeMillis();
    final InMemoryStateInternalsFactory stateInternalsFactory = stateCoder.decode(inStream);

    final long st2 = System.currentTimeMillis();

    final DataInputStream dis = new DataInputStream(inStream);

    final Watermark prevOutputWatermark = new Watermark(dis.readLong());
    final Watermark inputWatermark = new Watermark(dis.readLong());

    final Map<K, Watermark> keyAndWatermarkMap = decodeKeyAndWatermarkMap(dis);

    final long st3 = System.currentTimeMillis();

    LOG.info("Decoding time: timer: {}, state: {}, keyWatermark: {}", (st1 - st), (st2 - st1), (st3 - st2));

    final GBKState finalState = new GBKState(
      timerInternalsFactory,
      stateInternalsFactory,
      prevOutputWatermark,
      keyAndWatermarkMap,
      inputWatermark);

    return finalState;
  }

  private void encodeKeyAndWatermarkMap(final Map<K, Watermark> map,
                                        final DataOutputStream dos) throws IOException {
    dos.writeInt(map.size());

    for (final Map.Entry<K, Watermark> entry : map.entrySet()) {
      keyCoder.encode(entry.getKey(), dos);
      dos.writeLong(entry.getValue().getTimestamp());
    }
  }

  private Map<K, Watermark> decodeKeyAndWatermarkMap(final DataInputStream dis) throws IOException {
    final int size = dis.readInt();
    final Map<K, Watermark> map = new HashMap<>();

    for (int i = 0; i < size; i++) {
      final K key = keyCoder.decode(dis);
      final Watermark watermark = new Watermark(dis.readLong());
      map.put(key, watermark);
    }

    return map;
  }


  @Override
  public List<? extends Coder<?>> getCoderArguments() {
    return null;
  }

  @Override
  public void verifyDeterministic() throws NonDeterministicException {
    keyCoder.verifyDeterministic();
    windowCoder.verifyDeterministic();
  }
}
