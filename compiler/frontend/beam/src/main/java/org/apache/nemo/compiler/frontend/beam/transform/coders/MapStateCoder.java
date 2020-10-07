package org.apache.nemo.compiler.frontend.beam.transform.coders;

import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.CoderException;
import org.apache.beam.sdk.state.MapState;
import org.apache.nemo.compiler.frontend.beam.transform.InMemoryStateInternals;

import java.io.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;

public final class MapStateCoder<KeyT, ValueT> extends Coder<MapState<KeyT, ValueT>> {

  private final Coder<KeyT> keyCoder;
  private final Coder<ValueT> valueCoder;

  public MapStateCoder(final Coder<KeyT> keyCoder,
                       final Coder<ValueT> valueCoder) {
    this.keyCoder = keyCoder;
    this.valueCoder = valueCoder;
  }

  @Override
  public void encode(MapState<KeyT, ValueT> value, OutputStream outStream) throws CoderException, IOException {
    final Iterable<Map.Entry<KeyT, ValueT>> iterable = value.entries().read();

    if (iterable == null) {
      outStream.write(1);
    } else {
      outStream.write(0);

      final List<Map.Entry<KeyT, ValueT>> l = new ArrayList<>();
      for (final Map.Entry<KeyT, ValueT> val : iterable) {
        l.add(val);
      }

      final DataOutputStream dos = new DataOutputStream(outStream);
      dos.writeInt(l.size());

      for (final Map.Entry<KeyT, ValueT> val : l) {
        keyCoder.encode(val.getKey(), outStream);
        valueCoder.encode(val.getValue(), outStream);
      }
    }
  }

  @Override
  public MapState<KeyT, ValueT> decode(InputStream inStream) throws CoderException, IOException {
    final int b = inStream.read();
    final MapState<KeyT, ValueT> state = new InMemoryStateInternals.InMemoryMap<>(keyCoder, valueCoder);

    if (b == 0) {
      final DataInputStream dis = new DataInputStream(inStream);
      final int size = dis.readInt();

      for (int i = 0; i < size; i++) {
        final KeyT key = keyCoder.decode(inStream);
        final ValueT value = valueCoder.decode(inStream);
        state.put(key, value);
      }
    }

    return state;
  }

  @Override
  public List<? extends Coder<?>> getCoderArguments() {
    return keyCoder.getCoderArguments();
  }

  @Override
  public void verifyDeterministic() throws Coder.NonDeterministicException {
    keyCoder.verifyDeterministic();
    valueCoder.verifyDeterministic();
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    MapStateCoder<?, ?> that = (MapStateCoder<?, ?>) o;
    return Objects.equals(keyCoder, that.keyCoder) &&
      Objects.equals(valueCoder, that.valueCoder);
  }

  @Override
  public int hashCode() {

    return Objects.hash(keyCoder, valueCoder);
  }

  @Override
  public String toString() {
    return keyCoder.toString() + ": " + valueCoder.toString();
  }
}
