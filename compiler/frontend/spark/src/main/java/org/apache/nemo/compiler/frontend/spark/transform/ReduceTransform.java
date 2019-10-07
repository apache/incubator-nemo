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
package org.apache.nemo.compiler.frontend.spark.transform;

import org.apache.nemo.common.ir.OutputCollector;
import org.apache.nemo.common.ir.vertex.transform.Transform;
import org.apache.nemo.common.punctuation.Watermark;
import org.apache.spark.api.java.function.Function2;

import javax.annotation.Nullable;
import java.util.Iterator;

/**
 * Reduce Transform for Spark.
 *
 * @param <T> element type.
 */
public final class ReduceTransform<T> implements Transform<T, T> {
  private final Function2<T, T, T> func;
  private OutputCollector<T> outputCollector;
  private T result;

  /**
   * Constructor.
   *
   * @param func function to run for the reduce transform.
   */
  public ReduceTransform(final Function2<T, T, T> func) {
    this.func = func;
    this.result = null;
  }

  @Override
  public void prepare(final Context context, final OutputCollector<T> oc) {
    this.outputCollector = oc;
  }

  @Override
  public void onData(final T element) {
    if (element == null) { // nothing to be done.
      return;
    }

    try {
      if (result == null) {
        result = element;
      }

      result = func.call(result, element);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }

    outputCollector.emit(result);
  }

  @Override
  public void onWatermark(final Watermark watermark) {
    outputCollector.emitWatermark(watermark);
  }

  /**
   * Reduce the iterator elements into a single object.
   *
   * @param elements the iterator of elements.
   * @param func     function to apply for reduction.
   * @param <T>      type of the elements.
   * @return the reduced element.
   */
  @Nullable
  public static <T> T reduceIterator(final Iterator<T> elements, final Function2<T, T, T> func) {
    if (!elements.hasNext()) { // nothing to be done
      return null;
    }

    T res = elements.next();
    while (elements.hasNext()) {
      try {
        res = func.call(res, elements.next());
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    }
    return res;
  }

  @Override
  public void close() {
    // Nothing to do in a SparkTransform.
  }
}
