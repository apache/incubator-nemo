package org.apache.nemo.compiler.frontend.beam.transform;

import org.apache.beam.sdk.coders.CannotProvideCoderException;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.CoderRegistry;
import org.apache.beam.sdk.transforms.Combine;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;

public final class PartialCombineFn<InputT, AccumT> extends Combine.CombineFn<InputT, AccumT, AccumT> {
  private static final Logger LOG = LoggerFactory.getLogger(PartialCombineFn.class.getName());
  private final Combine.CombineFn<InputT, AccumT, ?> originFn;
  private final Coder<AccumT> accumCoder;

  public PartialCombineFn(Combine.CombineFn<InputT, AccumT, ?> originFn,
                          final Coder<AccumT> accumCoder) {
    this.originFn = originFn;
    this.accumCoder = accumCoder;
  }

  @Override
  public AccumT createAccumulator() {
    return originFn.createAccumulator();
  }

  @Override
  public AccumT addInput(AccumT accumulator, InputT input) {
    //LOG.info("Add partial input: {}, {}", accumulator, input);
    return originFn.addInput(accumulator, input);
  }

  @Override
  public AccumT mergeAccumulators(Iterable<AccumT> accumulators) {
    //LOG.info("Merge partial accum: {}, {}", accumulators);
    return originFn.mergeAccumulators(accumulators);
  }

  @Override
  public AccumT extractOutput(AccumT accumulator) {
    //LOG.info("Extract output: {}", accumulator);
    return accumulator;
  }

  @Override
  public Coder<AccumT> getAccumulatorCoder(CoderRegistry registry, Coder<InputT> inputCoder) throws CannotProvideCoderException {
    return accumCoder;
    //return originFn.getAccumulatorCoder(registry, inputCoder);
  }
}
