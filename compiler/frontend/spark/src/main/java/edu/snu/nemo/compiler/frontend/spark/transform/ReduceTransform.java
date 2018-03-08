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

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Output;
import edu.snu.nemo.common.ir.OutputCollector;
import edu.snu.nemo.common.ir.vertex.transform.Transform;
import edu.snu.nemo.compiler.frontend.spark.core.java.JavaRDD;
import org.apache.spark.api.java.function.Function2;

import javax.annotation.Nullable;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
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
  private String filename;

  /**
   * Constructor.
   * @param func function to run for the reduce transform.
   */
  public ReduceTransform(final Function2<T, T, T> func) {
    this.func = func;
    this.result = null;
    this.filename = filename + JavaRDD.getResultId();
  }

  @Override
  public void prepare(final Context context, final OutputCollector<T> p) {
    this.outputCollector = p;
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

  /**
   * Reduce the iterator elements into a single object.
   * @param elements the iterator of elements.
   * @param func function to apply for reduction.
   * @param <T> type of the elements.
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
    // Write result to a temporary file.
    // TODO #711: remove this part, and make it properly write to sink.
    try {
      final Kryo kryo = new Kryo();
      final Output output = new Output(new FileOutputStream(filename));
      kryo.writeClassAndObject(output, result);
      output.close();
    } catch (FileNotFoundException e) {
      throw new RuntimeException(e);
    }
  }
}
