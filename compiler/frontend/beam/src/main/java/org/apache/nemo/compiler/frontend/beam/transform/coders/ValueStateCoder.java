/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
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

/**
 * Coder for {@link ValueState}.
 * @param <T> element type
 */
public final class ValueStateCoder<T> extends Coder<ValueState<T>> {

  private final Coder<T> coder;

  public ValueStateCoder(final Coder<T> coder) {
    this.coder = coder;
  }

  @Override
  public void encode(final ValueState<T> value, final OutputStream outStream) throws CoderException, IOException {
    final T val = value.read();

    if (val == null) {
      outStream.write(1);
    } else {
      outStream.write(0);
      coder.encode(val, outStream);
    }
  }

  @Override
  public ValueState<T> decode(final InputStream inStream) throws CoderException, IOException {

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
  public boolean equals(final Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
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
