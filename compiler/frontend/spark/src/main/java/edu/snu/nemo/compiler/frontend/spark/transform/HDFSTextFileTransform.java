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
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.JobConf;

import java.io.IOException;
import java.util.Iterator;
import java.util.UUID;

/**
 * Transform which saves elements to a HDFS text file for Spark.
 * @param <I> input type.
 * @param <O> output type.
 */
public final class HDFSTextFileTransform<I, O> implements Transform<I, O> {
  private final String path;
  private Path fileName;
  private FileSystem fileSystem;
  private FSDataOutputStream outputStream;

  /**
   * Constructor.
   *
   * @param path the path to write elements.
   */
  public HDFSTextFileTransform(final String path) {
    this.path = path;
  }

  @Override
  public void prepare(final Context context, final OutputCollector<O> outputCollector) {
    fileName = new Path(path + UUID.randomUUID().toString());
    try {
      fileSystem = fileName.getFileSystem(new JobConf());
      outputStream = fileSystem.create(fileName, false);
    } catch (final IOException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void onData(final Iterator<I> elements, final String srcVertexId) {
    elements.forEachRemaining(element -> {
      try {
        outputStream.writeBytes(element + "\n");
      } catch (final IOException e) {
        try {
          outputStream.close();
          fileSystem.delete(fileName, true);
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
      outputStream.close();
    } catch (final IOException e) {
      throw new RuntimeException(e);
    }
  }
}
