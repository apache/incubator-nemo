package org.apache.nemo.compiler.frontend.beam.transform.coders;

import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.CoderException;
import org.apache.beam.sdk.state.BagState;
import org.apache.beam.sdk.state.SetState;
import org.apache.nemo.compiler.frontend.beam.transform.InMemoryStateInternals;

import java.io.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

public final class SetStateCoder<T> extends Coder<SetState<T>> {

  private final Coder<T> coder;

  public SetStateCoder(final Coder<T> coder) {
    this.coder = coder;
  }

  @Override
  public void encode(SetState<T> value, OutputStream outStream) throws CoderException, IOException {
    final Iterable<T> iterable = value.read();

    if (iterable == null) {
      outStream.write(1);
    } else {
      outStream.write(0);
      final List<T> list = new ArrayList<>();
      iterable.forEach(elem -> list.add(elem));

      final DataOutputStream dos = new DataOutputStream(outStream);
      dos.writeInt(list.size());
      for (final T elem : list) {
        coder.encode(elem, outStream);
      }
    }
  }

  @Override
  public SetState<T> decode(InputStream inStream) throws CoderException, IOException {

    final int b = inStream.read();
    final SetState<T> state = new InMemoryStateInternals.InMemorySet<>(coder);

    if (b == 0) {
      final DataInputStream dis = new DataInputStream(inStream);
      final int size = dis.readInt();
      for (int i = 0; i < size; i++) {
        state.add(coder.decode(inStream));
      }
    }

    return state;

  }

  @Override
  public List<? extends Coder<?>> getCoderArguments() {
    return coder.getCoderArguments();
  }

  @Override
  public void verifyDeterministic() throws NonDeterministicException {
    coder.verifyDeterministic();
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    SetStateCoder<?> that = (SetStateCoder<?>) o;
    return Objects.equals(coder, that.coder);
  }

  @Override
  public int hashCode() {

    return Objects.hash(coder);
  }

  @Override
  public String toString() {
    return coder.toString();
  }
}
