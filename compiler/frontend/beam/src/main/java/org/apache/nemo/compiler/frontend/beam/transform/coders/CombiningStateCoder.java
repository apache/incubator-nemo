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
import org.apache.beam.sdk.state.CombiningState;
import org.apache.beam.sdk.transforms.Combine;
//import org.apache.commons.lang3.SerializationUtils;
//import org.apache.nemo.compiler.frontend.beam.transform.GBKFinalTransform;
import org.apache.nemo.compiler.frontend.beam.transform.InMemoryStateInternals;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.List;
import java.util.Objects;

/**
 * Coder for {@link CombiningState}.
 * @param <InputT> input type
 * @param <AccumT> accumulator type
 * @param <OutputT> output type
 */
public final class CombiningStateCoder<InputT, AccumT, OutputT> extends Coder<CombiningState<InputT, AccumT, OutputT>> {
  private static final Logger LOG = LoggerFactory.getLogger(CombiningStateCoder.class.getName());
  private final Coder<AccumT> coder;
  private final Combine.CombineFn<InputT, AccumT, OutputT> combineFn;

  @Override
  public boolean equals(final Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    CombiningStateCoder<?, ?, ?> that = (CombiningStateCoder<?, ?, ?>) o;
    return Objects.equals(coder, that.coder)
      && Objects.equals(combineFn, that.combineFn);
  }

  @Override
  public int hashCode() {

    return Objects.hash(coder, combineFn);
  }

  public CombiningStateCoder(final Coder<AccumT> coder,
                             final Combine.CombineFn<InputT, AccumT, OutputT> combineFn) {
    this.coder = coder;
    this.combineFn = combineFn;
  }

  @Override
  public void encode(final CombiningState<InputT, AccumT, OutputT> value, final OutputStream outStream)
    throws CoderException, IOException {
    final AccumT state = value.getAccum();
    //LOG.info("Combining state: {}", state);

    coder.encode(state, outStream);
  }

  @Override
  public CombiningState<InputT, AccumT, OutputT> decode(final InputStream inStream) throws CoderException, IOException {
    final AccumT accum = coder.decode(inStream);
    final CombiningState<InputT, AccumT, OutputT> state =
      new InMemoryStateInternals.InMemoryCombiningState<>(combineFn, coder);
    state.addAccum(accum);
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
