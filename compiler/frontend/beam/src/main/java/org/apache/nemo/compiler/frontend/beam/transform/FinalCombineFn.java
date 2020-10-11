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

import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.CoderRegistry;
import org.apache.beam.sdk.transforms.Combine;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;

/**
 * Wrapper class for {@link Combine.CombineFn}.
 * When adding input, it merges its accumulator and input accumulator into a single accumulator.
 * @param <AccumT> accumulator type
 * @param <Output> output type
 */
public final class FinalCombineFn<AccumT, Output> extends Combine.CombineFn<AccumT, AccumT, Output> {
  private static final Logger LOG = LoggerFactory.getLogger(FinalCombineFn.class.getName());
  private final Combine.CombineFn<?, AccumT, Output> originFn;
  private final Coder<AccumT> accumCoder;

  public FinalCombineFn(final Combine.CombineFn<?, AccumT, Output> originFn,
                        final Coder<AccumT> accumCoder) {
    this.originFn = originFn;
    this.accumCoder = accumCoder;
  }

  @Override
  public AccumT createAccumulator() {
    return originFn.createAccumulator();
  }

  @Override
  public AccumT addInput(final AccumT accumulator, final AccumT input) {
    final AccumT result = originFn.mergeAccumulators(Arrays.asList(accumulator, input));
    return result;
  }

  @Override
  public Coder<AccumT> getAccumulatorCoder(final CoderRegistry registry, final Coder<AccumT> ac) {
    return accumCoder;
  }

  @Override
  public AccumT mergeAccumulators(final Iterable<AccumT> accumulators) {
    return originFn.mergeAccumulators(accumulators);
  }

  @Override
  public Output extractOutput(final AccumT accumulator) {
    final Output result = originFn.extractOutput(accumulator);
    return result;
  }
}
