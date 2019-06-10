package org.apache.nemo.compiler.frontend.beam.transform.coders;

import org.apache.beam.runners.core.StateInternals;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.CoderException;
import org.apache.nemo.common.Pair;
import org.apache.nemo.common.coder.FSTSingleton;
import org.apache.nemo.compiler.frontend.beam.transform.InMemoryStateInternals;
import org.apache.nemo.compiler.frontend.beam.transform.InMemoryStateInternalsFactory;
import org.apache.nemo.compiler.frontend.beam.transform.NemoStateBackend;
import org.nustaq.serialization.FSTConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.*;
import java.util.stream.Collectors;

public final class InMemoryStateInternalsFactoryCoder<K> extends Coder<InMemoryStateInternalsFactory<K>> {
  private static final Logger LOG = LoggerFactory.getLogger(InMemoryStateInternalsFactoryCoder.class.getName());

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

    final Set<Coder> coderSet =
      stateBackendMap.values().stream()
        .flatMap(stateBackend -> stateBackend.map.values().stream())
        .flatMap(m -> m.values().stream())
        .map(Pair::right)
        .collect(Collectors.toSet());

    final Map<Coder, Integer> indexCoderMap = new HashMap<>();

    final List<Coder> coderList = new ArrayList<>(coderSet);

    for (int i = 0; i < coderList.size(); i++) {
      indexCoderMap.put(coderList.get(i), i);
    }

    LOG.info("# of keys for state internals: {}, coder size: {}", stateBackendMap.size(), coderList.size());
    dos.writeInt(coderList.size());

    // encoding coders
    final FSTConfiguration conf = FSTSingleton.getInstance();

    for (int i = 0; i < coderList.size(); i++) {
      conf.encodeToStream(dos, coderList.get(i));
    }

    for (final Map.Entry<K, NemoStateBackend> entry : stateBackendMap.entrySet()) {
      final K key = entry.getKey();
      final NemoStateBackend val = entry.getValue();
      keyCoder.encode(key, dos);
      nemoStateBackendCoder.encode(val, dos, indexCoderMap);
    }
  }

  @Override
  public InMemoryStateInternalsFactory<K> decode(InputStream inStream) throws CoderException, IOException {

    final DataInputStream dis = new DataInputStream(inStream);
    final int size = dis.readInt();
    final Map<K, NemoStateBackend> map = new HashMap<>();
    final Map<K, StateInternals> map2 = new HashMap<>();

    final int coderSize = dis.readInt();
    LOG.info("Coder size: {}", coderSize);

    final List<Coder> coderList = new ArrayList<>(coderSize);

    final FSTConfiguration conf = FSTSingleton.getInstance();
    for (int i = 0; i < coderSize; i++) {
      final Coder coder;
      try {
        coder = (Coder) conf.decodeFromStream(inStream);
      } catch (Exception e) {
        e.printStackTrace();
        throw new RuntimeException(e);
      }
      coderList.add(coder);
    }

    for (int i = 0; i < size; i++) {
      final K key = keyCoder.decode(dis);
      final NemoStateBackend val = nemoStateBackendCoder.decode(dis, coderList);
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
