package org.apache.nemo.compiler.frontend.beam.transform.coders;

import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.CoderException;
import org.apache.commons.lang3.SerializationUtils;
import org.apache.nemo.common.punctuation.Watermark;
import org.apache.nemo.compiler.frontend.beam.transform.GBKFinalState;
import org.apache.nemo.compiler.frontend.beam.transform.InMemoryStateInternalsFactory;
import org.apache.nemo.compiler.frontend.beam.transform.InMemoryTimerInternalsFactory;
import org.joda.time.Instant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public final class GBKFinalStateCoder<K> extends Coder<GBKFinalState<K>> {
  private static final Logger LOG = LoggerFactory.getLogger(GBKFinalStateCoder.class.getName());

  private final Coder<K> keyCoder;
  private final Coder windowCoder;
  private final InMemoryTimerInternalsFactoryCoder<K> timerCoder;
  private final InMemoryStateInternalsFactoryCoder<K> stateCoder;

  public GBKFinalStateCoder(final Coder<K> keyCoder,
                            final Coder windowCoder) {
    this.timerCoder = new InMemoryTimerInternalsFactoryCoder<>(keyCoder, windowCoder);
    this.stateCoder = new InMemoryStateInternalsFactoryCoder<>(keyCoder, windowCoder);
    this.keyCoder = keyCoder;
    this.windowCoder = windowCoder;
  }

  @Override
  public void encode(GBKFinalState<K> value, OutputStream outStream) throws CoderException, IOException {
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
  public GBKFinalState<K> decode(InputStream inStream) throws CoderException, IOException {


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

    final GBKFinalState finalState = new GBKFinalState(
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
