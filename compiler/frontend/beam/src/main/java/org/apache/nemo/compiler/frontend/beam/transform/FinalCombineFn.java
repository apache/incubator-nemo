package org.apache.nemo.compiler.frontend.beam.transform;

import org.apache.beam.sdk.transforms.Combine;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;

public final class FinalCombineFn<AccumT, Output> extends Combine.CombineFn<AccumT, AccumT, Output> {
  private static final Logger LOG = LoggerFactory.getLogger(FinalCombineFn.class.getName());
  private final Combine.CombineFn<?, AccumT, Output> originFn;

  public FinalCombineFn(Combine.CombineFn<?, AccumT, Output> originFn) {
    this.originFn = originFn;
  }

  @Override
  public AccumT createAccumulator() {
    return originFn.createAccumulator();
  }

  @Override
  public AccumT addInput(AccumT accumulator, AccumT input) {
    //LOG.info("Add input!! {}, {}", accumulator, input);
    final AccumT result = originFn.mergeAccumulators(Arrays.asList(accumulator, input));
    //LOG.info("Result!! {}", result);
    return result;
  }

  @Override
  public AccumT mergeAccumulators(Iterable<AccumT> accumulators) {
    return originFn.mergeAccumulators(accumulators);
  }

  @Override
  public Output extractOutput(AccumT accumulator) {
    final Output result = originFn.extractOutput(accumulator);
    //LOG.info("Extract output {}: {}", accumulator, result);
    return result;
  }
}
