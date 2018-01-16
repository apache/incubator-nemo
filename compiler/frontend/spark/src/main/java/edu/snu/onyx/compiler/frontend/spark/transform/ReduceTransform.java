/*
 * Copyright (C) 2017 Seoul National University
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
package edu.snu.onyx.compiler.frontend.spark.transform;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Output;
import edu.snu.onyx.common.ir.OutputCollector;
import edu.snu.onyx.common.ir.vertex.transform.Transform;
import edu.snu.onyx.compiler.frontend.spark.JavaRDD;
import org.apache.spark.api.java.function.Function2;

import javax.annotation.Nullable;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.util.Iterator;

/**
 * Reduce Transform for Spark.
 * @param <T> element type.
 */
public final class ReduceTransform<T> implements Transform<T, T> {
  private final Function2<T, T, T> func;
  private OutputCollector<T> oc;
  private String filename;

  /**
   * Constructor.
   * @param func function to run for the reduce transform.
   * @param filename file to keep the result in.
   */
  public ReduceTransform(final Function2<T, T, T> func, final String filename) {
    this.func = func;
    this.filename = filename;
  }

  @Override
  public void prepare(final Context context, final OutputCollector<T> outputCollector) {
    this.oc = outputCollector;
    this.filename = filename + JavaRDD.getResultId();
  }

  @Override
  public void onData(final Iterator<T> elements, final String srcVertexId) {
    final T res = reduceIterator(elements, func);
    if (res == null) { // nothing to be done.
      return;
    }

    oc.emit(res);

    // Write result to a temporary file.
    // TODO #711: remove this part, and make it properly write to sink.
    try {
      final Kryo kryo = new Kryo();
      final Output output = new Output(new FileOutputStream(filename));
      kryo.writeClassAndObject(output, res);
      output.close();
    } catch (FileNotFoundException e) {
      throw new RuntimeException(e);
    }
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
  }
}
