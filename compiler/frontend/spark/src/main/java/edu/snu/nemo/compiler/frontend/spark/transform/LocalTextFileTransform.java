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

import java.io.*;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

/**
 * Transform which saves elements to a local text file for Spark.
 * @param <I> input type.
 */
public final class LocalTextFileTransform<I> implements Transform<I, String> {
  private final String path;
  private String fileName;
  private List<I> elements;

  /**
   * Constructor.
   *
   * @param path the path to write elements.
   */
  public LocalTextFileTransform(final String path) {
    this.path = path;
  }

  @Override
  public void prepare(final Context context, final OutputCollector<String> outputCollector) {
    fileName = path + UUID.randomUUID().toString();
    this.elements = new ArrayList<>();
  }

  @Override
  public void onData(final I element) {
    elements.add(element);
  }

  @Override
  public void close() {
    try (
        final Writer writer =
            new BufferedWriter(new OutputStreamWriter(new FileOutputStream(fileName, false), "utf-8"))
    ) {
      for (final I element : elements) {
        writer.write(element + "\n");
      }
      writer.close();
    } catch (final IOException e) {
      throw new RuntimeException(e);
    }
  }
}
