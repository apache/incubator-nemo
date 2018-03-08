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
import edu.snu.nemo.compiler.frontend.spark.core.java.JavaRDD;

import java.io.FileOutputStream;
import java.io.ObjectOutputStream;
import java.util.ArrayList;
import java.util.List;

/**
 * Collect transform.
 * @param <T> type of data to collect.
 */
public final class CollectTransform<T> implements Transform<T, T> {
  private String filename;
  private FileOutputStream fos;
  private ObjectOutputStream oos;
  private final List<T> list;

  /**
   * Constructor.
   *
   * @param filename file to keep the result in.
   */
  public CollectTransform(final String filename) {
    this.filename = filename;
    this.list = new ArrayList<>();
  }

  @Override
  public void prepare(final Context context, final OutputCollector<T> p) {
    this.filename = filename + JavaRDD.getResultId();
  }

  @Override
  public void onData(final T element) {
    // Write result to a temporary file.
    // TODO #740: remove this part, and make it properly transfer with executor.
    list.add(element);
  }

  @Override
  public void close() {
    try {
      fos = new FileOutputStream(filename);
      oos = new ObjectOutputStream(fos);
      oos.writeObject(list);
      oos.close();
    } catch (Exception e) {
      throw new RuntimeException("Exception while file closing in CollectTransform " + e);
    }
  }
}
