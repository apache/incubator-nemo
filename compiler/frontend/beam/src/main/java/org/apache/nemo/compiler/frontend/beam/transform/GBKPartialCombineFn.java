package org.apache.nemo.compiler.frontend.beam.transform;

import org.apache.beam.sdk.coders.CannotProvideCoderException;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.CoderRegistry;
import org.apache.beam.sdk.transforms.Combine;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.LinkedList;
import java.util.List;

public final class GBKPartialCombineFn<InputT> extends Combine.CombineFn<InputT,
  Collection<InputT>, Collection<InputT>> {
  private static final Logger LOG = LoggerFactory.getLogger(GBKPartialCombineFn.class.getName());
  private final Coder<Collection<InputT>> accumCoder;

  public GBKPartialCombineFn(final Coder accumCoder) {
    this.accumCoder = accumCoder;
  }

  @Override
  public Collection<InputT> createAccumulator() {
    return new LinkedList<>();
  }

  @Override
  public Coder<Collection<InputT>> getAccumulatorCoder(CoderRegistry registry,
                                                       Coder<InputT> ac) {
    LOG.info("Get accumCoder: {}", accumCoder);
    return accumCoder;
  }

  @Override
  public Collection<InputT> addInput(Collection<InputT> accumulator, InputT input) {
    accumulator.add(input);
    return accumulator;
  }

  @Override
  public Collection<InputT> mergeAccumulators(Iterable<Collection<InputT>> accumulators) {
    throw new RuntimeException("Not supported");
  }

  @Override
  public Collection<InputT> extractOutput(Collection<InputT> accumulator) {
    return accumulator;
  }
}
