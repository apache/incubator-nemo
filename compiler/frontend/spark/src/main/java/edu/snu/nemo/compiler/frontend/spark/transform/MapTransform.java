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
import org.apache.spark.api.java.function.Function;

/**
 * Map Transform for Spark.
 * @param <I> input type.
 * @param <O> output type.
 */
public final class MapTransform<I, O> implements Transform<I, O> {
  private final Function<I, O> func;
  private OutputCollector<O> outputCollector;

  /**
   * Constructor.
   * @param func the function to run map with.
   */
  public MapTransform(final Function<I, O> func) {
    this.func = func;
  }

  @Override
  public void prepare(final Context context, final OutputCollector<O> p) {
    this.outputCollector = p;
  }

  public void onData(final Object element) {
      try {
        outputCollector.emit(func.call((I) element));
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
  }

  @Override
  public void close() {
  }
}
