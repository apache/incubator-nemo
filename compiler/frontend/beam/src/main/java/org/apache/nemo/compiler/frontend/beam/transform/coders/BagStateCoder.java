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
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.state.BagState;
import org.apache.nemo.compiler.frontend.beam.transform.InMemoryStateInternals;

import java.io.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/**
 * Coder for {@link BagState}.
 * @param <T> element type
 */
public final class BagStateCoder<T> extends Coder<BagState<T>> {

  private final Coder<T> coder;

  @Override
  public boolean equals(final Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    BagStateCoder<?> that = (BagStateCoder<?>) o;
    return Objects.equals(coder, that.coder);
  }

  @Override
  public int hashCode() {

    return Objects.hash(coder);
  }

  public BagStateCoder(final Coder<T> coder) {
    if (coder instanceof KvCoder) {
      this.coder = ((KvCoder) coder).getValueCoder();
    } else {
      this.coder = coder;
    }
  }

  @Override
  public void encode(final BagState<T> value, final OutputStream outStream) throws CoderException, IOException {
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
  public BagState<T> decode(final InputStream inStream) throws CoderException, IOException {
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

  @Override
  public String toString() {
    return coder.toString();
  }
}
