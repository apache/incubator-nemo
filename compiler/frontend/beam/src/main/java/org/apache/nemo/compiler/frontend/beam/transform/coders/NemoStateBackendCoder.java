package org.apache.nemo.compiler.frontend.beam.transform.coders;

import org.apache.beam.runners.core.StateNamespace;
import org.apache.beam.runners.core.StateNamespaces;
import org.apache.beam.runners.core.StateTag;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.CoderException;
import org.apache.beam.sdk.state.State;
import org.apache.commons.lang3.SerializationUtils;
import org.apache.nemo.common.Pair;
import org.apache.nemo.compiler.frontend.beam.transform.NemoStateBackend;

import java.io.*;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public final class NemoStateBackendCoder extends Coder<NemoStateBackend> {

  private final Coder windowCoder;

  public NemoStateBackendCoder(final Coder windowCoder) {
    this.windowCoder = windowCoder;
  }

  @Override
  public void encode(NemoStateBackend value, OutputStream outStream) throws CoderException, IOException {
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
    final int size = stateMap.size();
    outputStream.writeInt(size);

    for (final Map.Entry<StateTag, Pair<State, Coder>> entry : stateMap.entrySet()) {
      final StateTag stateTag = entry.getKey();
      SerializationUtils.serialize(stateTag, outputStream);
      final Coder stateCoder = entry.getValue().right();
      final State state = entry.getValue().left();

      SerializationUtils.serialize(stateCoder, outputStream);
      stateCoder.encode(state, outputStream);
    }
  }

  @Override
  public NemoStateBackend decode(InputStream inStream) throws CoderException, IOException {
    final DataInputStream dis = new DataInputStream(inStream);
    final int size = dis.readInt();

    final Map<StateNamespace, Map<StateTag, Pair<State, Coder>>> map = new HashMap<>();

    for (int i = 0; i < size; i++) {
      final StateNamespace stateNamespace =
        StateNamespaces.fromString(dis.readUTF(), windowCoder);
      final Map<StateTag, Pair<State, Coder>> val = decodeMap(dis);

      map.put(stateNamespace, val);
    }

    return new NemoStateBackend(map);
  }

  private Map<StateTag, Pair<State, Coder>> decodeMap(final DataInputStream dis) throws IOException {
    final int size = dis.readInt();
    final Map<StateTag, Pair<State, Coder>> map = new HashMap<>();

    for (int i = 0; i < size; i++) {
      final StateTag stateTag = SerializationUtils.deserialize(dis);
      final Coder<State> stateCoder = SerializationUtils.deserialize(dis);
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
