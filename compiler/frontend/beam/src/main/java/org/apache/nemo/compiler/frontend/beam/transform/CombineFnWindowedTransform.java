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

import java.util.Map;

/**
 * This transform executes CombinePerKey transformation when input data is unbounded or is in a non-global window.
 * @param <K> key type
 * @param <InputT> input type
 * @param <OutputT> output type
 */
public final class CombineFnWindowedTransform<K, InputT, OutputT> extends GBKTransform<K, InputT, OutputT> {
  public CombineFnWindowedTransform(final Map<TupleTag<?>, Coder<?>> outputCoders,
                                    final TupleTag<KV<K, OutputT>> mainOutputTag,
                                    final WindowingStrategy<?, ?> windowingStrategy,
                                    final PipelineOptions options,
                                    final SystemReduceFn reduceFn,
                                    final DoFnSchemaInformation doFnSchemaInformation,
                                    final DisplayData displayData) {
    super(outputCoders, mainOutputTag, windowingStrategy, options, reduceFn, doFnSchemaInformation, displayData);
  }
}
