package org.apache.nemo.compiler.frontend.beam.transform.coders;

import org.apache.beam.runners.core.StateInternals;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.CoderException;
import org.apache.nemo.compiler.frontend.beam.transform.InMemoryStateInternals;
import org.apache.nemo.compiler.frontend.beam.transform.InMemoryStateInternalsFactory;
import org.apache.nemo.compiler.frontend.beam.transform.NemoStateBackend;

import java.io.*;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public final class InMemoryStateInternalsFactoryCoder<K> extends Coder<InMemoryStateInternalsFactory<K>> {

  private final Coder<K> keyCoder;
  private final Coder windowCoder;
  private final NemoStateBackendCoder nemoStateBackendCoder;

  public InMemoryStateInternalsFactoryCoder(final Coder<K> keyCoder,
                                            final Coder windowCoder) {
    this.keyCoder = keyCoder;
    this.windowCoder = windowCoder;
    this.nemoStateBackendCoder = new NemoStateBackendCoder(windowCoder);
  }

  @Override
  public void encode(InMemoryStateInternalsFactory<K> value, OutputStream outStream) throws CoderException, IOException {

    final Map<K, NemoStateBackend> stateBackendMap = value.stateBackendMap;


    final int size = stateBackendMap.size();
    final DataOutputStream dos = new DataOutputStream(outStream);
    dos.writeInt(size);

    for (final Map.Entry<K, NemoStateBackend> entry : stateBackendMap.entrySet()) {
      final K key = entry.getKey();
      final NemoStateBackend val = entry.getValue();
      keyCoder.encode(key, dos);
      nemoStateBackendCoder.encode(val, dos);
    }
  }

  @Override
  public InMemoryStateInternalsFactory<K> decode(InputStream inStream) throws CoderException, IOException {

    final DataInputStream dis = new DataInputStream(inStream);
    final int size = dis.readInt();
    final Map<K, NemoStateBackend> map = new HashMap<>();
    final Map<K, StateInternals> map2 = new HashMap<>();

    for (int i = 0; i < size; i++) {
      final K key = keyCoder.decode(dis);
      final NemoStateBackend val = nemoStateBackendCoder.decode(dis);
      map.put(key, val);

      final StateInternals internal = InMemoryStateInternals.forKey(key, val);
      map2.put(key, internal);
    }

    return new InMemoryStateInternalsFactory<>(map2, map);
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
