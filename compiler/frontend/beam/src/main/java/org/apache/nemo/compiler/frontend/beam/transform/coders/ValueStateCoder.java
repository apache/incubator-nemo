package org.apache.nemo.compiler.frontend.beam.transform.coders;

import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.CoderException;
import org.apache.beam.sdk.state.ValueState;
import org.apache.nemo.compiler.frontend.beam.transform.InMemoryStateInternals;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.List;
import java.util.Objects;

public final class ValueStateCoder<T> extends Coder<ValueState<T>> {

  private final Coder<T> coder;

  public ValueStateCoder(final Coder<T> coder) {
    this.coder = coder;
  }

  @Override
  public void encode(ValueState<T> value, OutputStream outStream) throws CoderException, IOException {
    final T val = value.read();

    if (val == null) {
      // if null, set 1
      outStream.write(1);
    } else {
      outStream.write(0);
      coder.encode(val, outStream);
    }
  }

  @Override
  public ValueState<T> decode(InputStream inStream) throws CoderException, IOException {

    final ValueState<T> valueState = new InMemoryStateInternals.InMemoryValue<>(coder);

    final int b = inStream.read();

    if (b == 0) {
      final T value = coder.decode(inStream);
      valueState.write(value);
    }

    return valueState;
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
    ValueStateCoder<?> that = (ValueStateCoder<?>) o;
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
