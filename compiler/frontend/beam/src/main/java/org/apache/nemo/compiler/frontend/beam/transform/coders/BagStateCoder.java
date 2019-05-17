package org.apache.nemo.compiler.frontend.beam.transform.coders;

import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.CoderException;
import org.apache.beam.sdk.state.BagState;
import org.apache.nemo.compiler.frontend.beam.transform.InMemoryStateInternals;

import java.io.*;
import java.util.ArrayList;
import java.util.List;

public final class BagStateCoder<T> extends Coder<BagState<T>> {

  private final Coder<T> coder;

  public BagStateCoder(final Coder<T> coder) {
    this.coder = coder;
  }

  @Override
  public void encode(BagState<T> value, OutputStream outStream) throws CoderException, IOException {
    final Iterable<T> iterable = value.read();
    final List<T> list = new ArrayList<>();
    iterable.forEach(elem -> list.add(elem));

    final DataOutputStream dos = new DataOutputStream(outStream);
    dos.writeInt(list.size());
    for (final T elem : list) {
      coder.encode(elem, outStream);
    }
  }

  @Override
  public BagState<T> decode(InputStream inStream) throws CoderException, IOException {
    final DataInputStream dis = new DataInputStream(inStream);
    final int size = dis.readInt();
    final BagState<T> state = new InMemoryStateInternals.InMemoryBag<>(coder);
    for (int i = 0; i < size; i++) {
      state.add(coder.decode(inStream));
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
}
