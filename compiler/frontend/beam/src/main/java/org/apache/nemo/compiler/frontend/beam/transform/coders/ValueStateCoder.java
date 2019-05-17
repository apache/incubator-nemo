package org.apache.nemo.compiler.frontend.beam.transform.coders;

import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.CoderException;
import org.apache.beam.sdk.state.ValueState;
import org.apache.nemo.compiler.frontend.beam.transform.InMemoryStateInternals;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.List;

public final class ValueStateCoder<T> extends Coder<ValueState<T>> {

  private final Coder<T> coder;

  public ValueStateCoder(final Coder<T> coder) {
    this.coder = coder;
  }

  @Override
  public void encode(ValueState<T> value, OutputStream outStream) throws CoderException, IOException {
    coder.encode(value.read(), outStream);
  }

  @Override
  public ValueState<T> decode(InputStream inStream) throws CoderException, IOException {
    final ValueState<T> valueState = new InMemoryStateInternals.InMemoryValue<>(coder);
    final T value = coder.decode(inStream);
    valueState.write(value);
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
}
