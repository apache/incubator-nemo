package org.apache.nemo.compiler.frontend.beam.transform.coders;

import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.CoderException;
import org.apache.commons.lang3.SerializationUtils;
import org.apache.nemo.common.punctuation.Watermark;
import org.apache.nemo.compiler.frontend.beam.transform.GBKFinalState;

import java.io.*;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public final class GBKFinalStateCoder<K> extends Coder<GBKFinalState<K>> {

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

    timerCoder.encode(value.timerInternalsFactory, outStream);
    stateCoder.encode(value.stateInternalsFactory, outStream);

    SerializationUtils.serialize(value.prevOutputWatermark, outStream);
    SerializationUtils.serialize(value.inputWatermark, outStream);

    encodeKeyAndWatermarkMap(value.keyAndWatermarkHoldMap, dos);
  }

  @Override
  public GBKFinalState<K> decode(InputStream inStream) throws CoderException, IOException {
    return null;
  }

  private void encodeKeyAndWatermarkMap(final Map<K, Watermark> map,
                                        final DataOutputStream dos) throws IOException {
    dos.writeInt(map.size());

    for (final Map.Entry<K, Watermark> entry : map.entrySet()) {
      keyCoder.encode(entry.getKey(), dos);
      SerializationUtils.serialize(entry.getValue(), dos);
    }
  }

  private Map<K, Watermark> decodeKeyAndWatermarkMap(final DataInputStream dis) throws IOException {
    final int size = dis.readInt();
    final Map<K, Watermark> map = new HashMap<>();

    for (int i = 0; i < size; i++) {
      final K key = keyCoder.decode(dis);
      final Watermark watermark = SerializationUtils.deserialize(dis);
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
