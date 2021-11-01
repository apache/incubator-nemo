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

import org.apache.beam.runners.core.SystemReduceFn;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.transforms.DoFnSchemaInformation;
import org.apache.beam.sdk.transforms.display.DisplayData;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.WindowingStrategy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.Map;

/**
 * Factory for the combine transform that combines the results during the group by key.
 * @param <K> the type of the key.
 * @param <InputT> the type of the input values.
 * @param <AccumT> the type of the accumulators.
 * @param <OutputT> the type of the output.
 */
public class CombineTransformFactory<K, InputT, AccumT, OutputT> {
  private static final Logger LOG = LoggerFactory.getLogger(CombineTransformFactory.class.getName());

  private final SystemReduceFn combineFn;
  private final SystemReduceFn intermediateCombineFn;
  private final SystemReduceFn finalReduceFn;

  private final Coder<KV<K, InputT>> inputCoder;
  private final TupleTag<KV<K, AccumT>> partialMainOutputTag;
  private final Coder<KV<K, AccumT>> accumulatorCoder;
  private final Map<TupleTag<?>, Coder<?>> outputCoders;

  private final TupleTag<KV<K, OutputT>> mainOutputTag;
  private final WindowingStrategy<?, ?> windowingStrategy;
  private final PipelineOptions options;

  private final DoFnSchemaInformation doFnSchemaInformation;
  private final DisplayData displayData;

  public CombineTransformFactory(final Coder<KV<K, InputT>> inputCoder,
                                 final TupleTag<KV<K, AccumT>> partialMainOutputTag,
                                 final Coder<KV<K, AccumT>> accumulatorCoder,
                                 final Map<TupleTag<?>, Coder<?>> outputCoders,
                                 final TupleTag<KV<K, OutputT>> mainOutputTag,
                                 final WindowingStrategy<?, ?> windowingStrategy,
                                 final PipelineOptions options,
                                 final SystemReduceFn combineFn,
                                 final SystemReduceFn intermediateCombineFn,
                                 final SystemReduceFn finalReduceFn,
                                 final DoFnSchemaInformation doFnSchemaInformation,
                                 final DisplayData displayData) {
    this.combineFn = combineFn;
    this.intermediateCombineFn = intermediateCombineFn;
    this.finalReduceFn = finalReduceFn;

    this.inputCoder = inputCoder;
    this.partialMainOutputTag = partialMainOutputTag;
    this.accumulatorCoder = accumulatorCoder;
    this.outputCoders = outputCoders;

    this.mainOutputTag = mainOutputTag;
    this.windowingStrategy = windowingStrategy;
    this.options = options;

    this.doFnSchemaInformation = doFnSchemaInformation;
    this.displayData = displayData;
  }


  /**
   * Get the partial combine transform of the combine transform.
   * @return the partial combine transform for the combine transform.
   */
  public CombineTransform<K, InputT, AccumT> getPartialCombineTransform() {
    return new CombineTransform<>(inputCoder,
      Collections.singletonMap(partialMainOutputTag, accumulatorCoder),
      partialMainOutputTag,
      windowingStrategy,
      options,
      combineFn,
      doFnSchemaInformation,
      displayData, true);
  }

  /**
   * Get the intermediate combine transform of the combine transform.
   * @return the intermediate combine transform for the combine transform.
   */
  public CombineTransform<K, AccumT, AccumT> getIntermediateCombineTransform() {
    return new CombineTransform<>(accumulatorCoder,
      Collections.singletonMap(partialMainOutputTag, accumulatorCoder),
      partialMainOutputTag,
      windowingStrategy,
      options,
      intermediateCombineFn,
      doFnSchemaInformation,
      displayData, false);
  }

  /**
   * Get the final combine transform of the combine transform.
   * @return the final combine transform for the combine transform.
   */
  public CombineTransform<K, AccumT, OutputT> getFinalCombineTransform() {
    return new CombineTransform<>(accumulatorCoder,
      outputCoders,
      mainOutputTag,
      windowingStrategy,
      options,
      finalReduceFn,
      doFnSchemaInformation,
      displayData, false, this.getIntermediateCombineTransform());
  }
}
