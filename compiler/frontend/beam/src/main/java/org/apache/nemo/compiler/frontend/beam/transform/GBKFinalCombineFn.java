package org.apache.nemo.compiler.frontend.beam.transform;

import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.CoderRegistry;
import org.apache.beam.sdk.transforms.Combine;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

public final class GBKFinalCombineFn<InputT> extends Combine.CombineFn<Collection<InputT>,
  Collection<InputT>, Collection<InputT>> {
  private static final Logger LOG = LoggerFactory.getLogger(GBKPartialCombineFn.class.getName());
  private final Coder<Collection<InputT>> accumCoder;

  public GBKFinalCombineFn(final Coder accumCoder) {
    this.accumCoder = accumCoder;
  }

  @Override
  public Collection<InputT> createAccumulator() {
    return new LinkedList<>();
  }

  @Override
  public Collection<InputT> addInput(Collection<InputT> accumulator, Collection<InputT> input) {
    final List<InputT> arr = new ArrayList<>(accumulator.size() + input.size());
    arr.addAll(accumulator);
    arr.addAll(input);
    return arr;
  }

  @Override
  public Coder<Collection<InputT>> getAccumulatorCoder(CoderRegistry registry,
                                                       Coder<Collection<InputT>> ac) {
    LOG.info("Get accumCoder: {}", accumCoder);
    return accumCoder;
  }

  @Override
  public Collection<InputT> mergeAccumulators(Iterable<Collection<InputT>> accumulators) {
    final List<Collection<InputT>> l = new LinkedList<>();
    final Iterator<Collection<InputT>> iterator = accumulators.iterator();

    int size = 0;
    while (iterator.hasNext()) {
      final Collection<InputT> n = iterator.next();
      l.add(n);
      size += n.size();
    }

    if (l.size() == 1) {
      return l.get(0);
    } else {
      // merge
      final Collection<InputT> coll = new ArrayList<>(size);
      l.forEach(c -> ((ArrayList<InputT>) coll).addAll(c));
      return coll;
    }
  }

  @Override
  public Collection<InputT> extractOutput(Collection<InputT> accumulator) {
    return accumulator;
  }
}
