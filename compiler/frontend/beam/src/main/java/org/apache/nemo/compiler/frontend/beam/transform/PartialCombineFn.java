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
package org.apache.nemo.compiler.frontend.beam.transform;

import org.apache.beam.sdk.coders.CannotProvideCoderException;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.CoderRegistry;
import org.apache.beam.sdk.transforms.Combine;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Wrapper class for {@link Combine.CombineFn}.
 * When invoked to output, it outputs its accumulator, instead of the output from its original combine function.
 * @param <InputT> input type
 * @param <AccumT> accumulator type
 */
public final class PartialCombineFn<InputT, AccumT> extends Combine.CombineFn<InputT, AccumT, AccumT> {
  private static final Logger LOG = LoggerFactory.getLogger(PartialCombineFn.class.getName());
  private final Combine.CombineFn<InputT, AccumT, ?> originFn;
  private final Coder<AccumT> accumCoder;

  public PartialCombineFn(final Combine.CombineFn<InputT, AccumT, ?> originFn,
                          final Coder<AccumT> accumCoder) {
    this.originFn = originFn;
    this.accumCoder = accumCoder;
  }

  @Override
  public AccumT createAccumulator() {
    return originFn.createAccumulator();
  }

  @Override
  public AccumT addInput(final AccumT accumulator, final InputT input) {
    return originFn.addInput(accumulator, input);
  }

  @Override
  public AccumT mergeAccumulators(final Iterable<AccumT> accumulators) {
    return originFn.mergeAccumulators(accumulators);
  }

  @Override
  public AccumT extractOutput(final AccumT accumulator) {
    return accumulator;
  }

  @Override
  public Coder<AccumT> getAccumulatorCoder(final CoderRegistry registry, final Coder<InputT> inputCoder)
    throws CannotProvideCoderException {
    return accumCoder;
  }
}
