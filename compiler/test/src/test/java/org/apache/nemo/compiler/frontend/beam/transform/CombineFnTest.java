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

import junit.framework.TestCase;
import org.apache.beam.sdk.coders.*;
import org.apache.beam.sdk.transforms.Combine;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.Arrays;

public class CombineFnTest extends TestCase {
  private static final Logger LOG = LoggerFactory.getLogger(CombineFnTest.class.getName());
  private final static Coder INTEGER_CODER = BigEndianIntegerCoder.of();

  // Define combine function.
  public static final class CountFn extends Combine.CombineFn<Integer, CountFn.Accum, Integer> {
    public static final class Accum {
      int sum = 0;

      @Override
      public boolean equals(Object o) {
        if (Accum.class != o.getClass()) {
          return false;
        }
        return (sum == ((Accum) o).sum);
      }
    }

    @Override
    public CountFn.Accum createAccumulator() {
      return new Accum();
    }

    @Override
    public CountFn.Accum addInput(final CountFn.Accum accum, final Integer input) {
      accum.sum += input;
      return accum;
    }

    @Override
    public CountFn.Accum mergeAccumulators(final Iterable<CountFn.Accum> accums) {
      Accum merged = createAccumulator();
      for (final Accum accum : accums) {
        merged.sum += accum.sum;
      }
      return merged;
    }

    @Override
    public Integer extractOutput(final CountFn.Accum accum) {
      return accum.sum;
    }

    @Override
    public Coder<CountFn.Accum> getAccumulatorCoder(final CoderRegistry registry, final Coder<Integer> inputcoder) {
      return AvroCoder.of(CountFn.Accum.class);
    }
  }

  final Combine.CombineFn<Integer, CountFn.Accum, Integer> combineFn = new CountFn();
  final Coder<CountFn.Accum> accumCoder;
  {
    try {
      accumCoder = combineFn.getAccumulatorCoder(CoderRegistry.createDefault(), INTEGER_CODER);
    } catch (CannotProvideCoderException e) {
      throw new RuntimeException("Failed to provide an accumulator coder");
    }
  }

  @Test
  public void testPartialCombineFn() {
    // Initialize partial combineFn.
    final PartialCombineFn<Integer, CountFn.Accum> partialCombineFn = new PartialCombineFn(combineFn, accumCoder);

    // Create accumulator.
    final CountFn.Accum accum1 = partialCombineFn.createAccumulator();
    final CountFn.Accum accum2 = partialCombineFn.createAccumulator();
    final CountFn.Accum accum3 = partialCombineFn.createAccumulator();

    // Check whether accumulators are initialized correctly.
    assertEquals(0, accum1.sum);
    assertEquals(0, accum2.sum);
    assertEquals(0, accum3.sum);

    // Add input to each accumulators.
    partialCombineFn.addInput(accum1, 1);
    partialCombineFn.addInput(accum2, 2);
    partialCombineFn.addInput(accum3, 3);

    // Check whether inputs are added correctly.
    assertEquals(1, accum1.sum);
    assertEquals(2, accum2.sum);
    assertEquals(3, accum3.sum);

    // Merge accumulators.
    final CountFn.Accum mergedAccum = partialCombineFn.mergeAccumulators(Arrays.asList(accum1, accum2, accum3));

    // Check whether accumulators are merged correctly.
    assertEquals(6, mergedAccum.sum);

    // Extract output. When invoked to extract output, partial combineFn returns its accumulator.
    assertEquals(mergedAccum, partialCombineFn.extractOutput(mergedAccum));

    // Get accumulator coder. Check if the accumulator coder from partial combineFn is equal
    // to the one from original combineFn.
    try {
      assertEquals(accumCoder, partialCombineFn.getAccumulatorCoder(CoderRegistry.createDefault(), INTEGER_CODER));
    } catch (CannotProvideCoderException e) {
      throw new RuntimeException("Failed to provide an accumulator coder");
    }
  }

  @Test
  public void testIntermediateCombineFn() {
    // Initialize intermediate combine function.
    final IntermediateCombineFn<CountFn.Accum> intermediateCombineFn =
      new IntermediateCombineFn<>(combineFn, accumCoder);

    // Create accumulator.
    final CountFn.Accum accum1 = intermediateCombineFn.createAccumulator();
    final CountFn.Accum accum2 = intermediateCombineFn.createAccumulator();
    final CountFn.Accum accum3 = intermediateCombineFn.createAccumulator();

    final CountFn.Accum expectedMergedAccum = intermediateCombineFn.createAccumulator();
    expectedMergedAccum.sum = 6;

    // Check whether accumulators are initialized correctly.
    assertEquals(0, accum1.sum);
    assertEquals(0, accum2.sum);
    assertEquals(0, accum3.sum);

    // Change the parameter for the sake of unit testing.
    accum1.sum = 1;
    accum2.sum = 2;
    accum3.sum = 3;

    // Add input. Intermediate combineFn's addInput method takes accumulators as input
    // and merges them into a single accumulator.
    final CountFn.Accum addedAccum = intermediateCombineFn.addInput(accum1, accum2);

    // Check whether inputs are added correctly.
    assertEquals(3, addedAccum.sum);

    // Merge accumulators.
    CountFn.Accum mergedAccum = intermediateCombineFn.mergeAccumulators(Arrays.asList(accum1, accum2, accum3));

    // Check whether accumulators are merged correctly.
    assertEquals(expectedMergedAccum, mergedAccum);

    // Extract output.
    assertEquals(expectedMergedAccum, intermediateCombineFn.extractOutput(mergedAccum));

    // Get accumulator coder. Check if the accumulator coder from intermediate combineFn is equal
    // to the one from original combineFn.
    try {
      assertEquals(accumCoder, intermediateCombineFn.getAccumulatorCoder(CoderRegistry.createDefault(), INTEGER_CODER));
    } catch (CannotProvideCoderException e) {
      throw new RuntimeException("Failed to provide an accumulator coder");
    }
  }

  @Test
  public void testFinalCombineFn() {
    // Initialize final combine function.
    final FinalCombineFn<CountFn.Accum, Integer> finalCombineFn = new FinalCombineFn(combineFn, accumCoder);

    // Create accumulator.
    final CountFn.Accum accum1 = finalCombineFn.createAccumulator();
    final CountFn.Accum accum2 = finalCombineFn.createAccumulator();
    final CountFn.Accum accum3 = finalCombineFn.createAccumulator();

    // Check whether accumulators are initialized correctly.
    assertEquals(0, accum1.sum);
    assertEquals(0, accum2.sum);
    assertEquals(0, accum3.sum);

    // Change the parameter for the sake of unit testing.
    accum1.sum = 1;
    accum2.sum = 2;
    accum3.sum = 3;

    // Add input. Final combineFn's addInput method takes accumulators as input
    // and merges them into a single accumulator.
    final CountFn.Accum addedAccum = finalCombineFn.addInput(accum1, accum2);

    // Check whether inputs are added correctly.
    assertEquals(3, addedAccum.sum);

    // Merge accumulators.
    CountFn.Accum mergedAccum = finalCombineFn.mergeAccumulators(Arrays.asList(accum1, accum2, accum3));

    // Check whether accumulators are merged correctly.
    assertEquals(6, mergedAccum.sum);

    // Extract output.
    // When invoked to extract output, final combineFn invokes extractOutput method of original combineFn.
    assertEquals(combineFn.extractOutput(mergedAccum), finalCombineFn.extractOutput(mergedAccum));

    // Get accumulator coder. Check if the accumulator coder from final combine function is equal
    // to the one from original combine function.
    assertEquals(accumCoder, finalCombineFn.getAccumulatorCoder(CoderRegistry.createDefault(), INTEGER_CODER));
  }
}
