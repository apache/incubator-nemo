/*
 * Copyright (C) 2018 Seoul National University
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package edu.snu.nemo.compiler.frontend.spark.transform;

import edu.snu.nemo.common.ir.OutputCollector;
import edu.snu.nemo.common.ir.vertex.transform.Transform;
import org.apache.spark.api.java.function.FlatMapFunction;

/**
 * Flatmap Transform that flattens each output element after mapping each elements to an iterator.
 * @param <T> input type.
 * @param <U> output type.
 */
public final class FlatMapTransform<T, U> implements Transform<T, U> {
  private final FlatMapFunction<T, U> func;
  private OutputCollector<U> outputCollector;

  /**
   * Constructor.
   * @param func flat map function.
   */
  public FlatMapTransform(final FlatMapFunction<T, U> func) {
    this.func = func;
  }

  @Override
  public void prepare(final Context context, final OutputCollector<U> p) {
    this.outputCollector = p;
  }

  @Override
  public void onData(final T element) {
    try {
      func.call(element).forEachRemaining(outputCollector::emit);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void close() {
  }
}
