package org.apache.nemo.compiler.frontend.beam.transform.coders;

import org.apache.beam.runners.core.StateNamespace;
import org.apache.beam.runners.core.StateNamespaces;
import org.apache.beam.runners.core.StateTag;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.CoderException;
import org.apache.beam.sdk.state.State;
import org.apache.commons.lang3.SerializationUtils;
import org.apache.nemo.common.Pair;
import org.apache.nemo.common.coder.FSTSingleton;
import org.apache.nemo.compiler.frontend.beam.transform.NemoStateBackend;
import org.nustaq.serialization.FSTConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.*;
import java.util.stream.Collectors;

public final class NemoStateBackendCoder extends Coder<NemoStateBackend> {
  private static final Logger LOG = LoggerFactory.getLogger(NemoStateBackendCoder.class.getName());

  private final Coder windowCoder;

  public NemoStateBackendCoder(final Coder windowCoder) {
    this.windowCoder = windowCoder;
  }

  @Override
  public void encode(NemoStateBackend value, OutputStream outStream) throws CoderException, IOException {
    final FSTConfiguration conf = FSTSingleton.getInstance();

    final Map<StateNamespace, Map<StateTag, Pair<State, Coder>>> map = value.map;
    final int size = map.size();
    final DataOutputStream dos = new DataOutputStream(outStream);
    dos.writeInt(size);

    for (final Map.Entry<StateNamespace, Map<StateTag, Pair<State, Coder>>> entry : map.entrySet()) {
      final StateNamespace stateNamespace = entry.getKey();
      final Map<StateTag, Pair<State, Coder>> val = entry.getValue();

      dos.writeUTF(stateNamespace.stringKey());
      encode(val, dos);
    }
  }

  private void encode(final Map<StateTag, Pair<State, Coder>> stateMap,
                      final DataOutputStream outputStream) throws IOException {
    final FSTConfiguration conf = FSTSingleton.getInstance();

    final int size = stateMap.size();
    outputStream.writeInt(size);

    final Set<Coder> coderSet = stateMap.values().stream()
      .map(pair -> pair.right()).collect(Collectors.toSet());

    final Map<Coder, Integer> indexCoderMap = new HashMap<>();

    final List<Coder> coderList = new ArrayList<>(coderSet);


    for (int i = 0; i < coderList.size(); i++) {
      indexCoderMap.put(coderList.get(i), i);
    }

    // encode coder size
    outputStream.writeInt(coderList.size());
    // encoding coders
    for (int i = 0; i < coderList.size(); i++) {
      conf.encodeToStream(outputStream, coderList.get(i));
    }

    for (final Map.Entry<StateTag, Pair<State, Coder>> entry : stateMap.entrySet()) {
      final StateTag stateTag = entry.getKey();

      conf.encodeToStream(outputStream, stateTag);

      final Coder stateCoder = entry.getValue().right();
      final Integer indexOfCoder = indexCoderMap.get(stateCoder);

      final State state = entry.getValue().left();

      outputStream.writeInt(indexOfCoder);
      stateCoder.encode(state, outputStream);
    }
  }

  @Override
  public NemoStateBackend decode(InputStream inStream) throws CoderException, IOException {
    final FSTConfiguration conf = FSTSingleton.getInstance();

    final DataInputStream dis = new DataInputStream(inStream);
    final int size = dis.readInt();


    final Map<StateNamespace, Map<StateTag, Pair<State, Coder>>> map = new HashMap<>();
    final int coderSize = dis.readInt();
    final List<Coder> coderList = new ArrayList<>(coderSize);

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
      final StateNamespace stateNamespace =
        StateNamespaces.fromString(dis.readUTF(), windowCoder);
      final Map<StateTag, Pair<State, Coder>> val;
      try {
        val = decodeMap(dis, coderList);
      } catch (Exception e) {
        e.printStackTrace();
        throw new RuntimeException(e);
      }

      map.put(stateNamespace, val);
    }

    return new NemoStateBackend(map);
  }

  private Map<StateTag, Pair<State, Coder>> decodeMap(
    final DataInputStream dis,
    final List<Coder> coders) throws Exception {
    final FSTConfiguration conf = FSTSingleton.getInstance();

    final int size = dis.readInt();
    final Map<StateTag, Pair<State, Coder>> map = new HashMap<>();

    for (int i = 0; i < size; i++) {
      final StateTag stateTag = (StateTag) conf.decodeFromStream(dis);
      final int indexOfCoder = dis.readInt();
      final Coder<State> stateCoder = (Coder<State>) coders.get(indexOfCoder);
      final State state = stateCoder.decode(dis);

      map.put(stateTag, Pair.of(state, stateCoder));
    }

    return map;
  }

  @Override
  public List<? extends Coder<?>> getCoderArguments() {
    return null;
  }

  @Override
  public void verifyDeterministic() throws NonDeterministicException {

  }
}
