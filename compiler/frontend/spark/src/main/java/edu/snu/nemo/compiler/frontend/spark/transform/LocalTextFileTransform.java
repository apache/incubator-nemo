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
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Iterator;
import java.util.UUID;

/**
 * Transform which saves elements to a local text file for Spark.
 * @param <I> input type.
 * @param <O> output type.
 */
public final class LocalTextFileTransform<I, O> implements Transform<I, O> {
  private final String path;
  private String fileName;
  private Writer writer;

  /**
   * Constructor.
   *
   * @param path the path to write elements.
   */
  public LocalTextFileTransform(final String path) {
    this.path = path;
  }

  @Override
  public void prepare(final Context context, final OutputCollector<O> outputCollector) {
    fileName = path + UUID.randomUUID().toString();
    try {
      writer = new BufferedWriter(new OutputStreamWriter(
          new FileOutputStream(fileName, false), "utf-8"));
    } catch (final IOException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void onData(final Iterator<I> elements, final String srcVertexId) {
    elements.forEachRemaining(element -> {
      try {
        writer.write(element + "\n");
      } catch (final IOException e) {
        try {
          writer.close();
          if (new File(fileName).exists()) {
            Files.delete(Paths.get(fileName));
          }
        } catch (final IOException e2) {
          throw new RuntimeException(e.toString() + e2.toString());
        }
        throw new RuntimeException(e);
      }
    });
  }

  @Override
  public void close() {
    try {
      writer.close();
    } catch (final IOException e) {
      throw new RuntimeException(e);
    }
  }
}
