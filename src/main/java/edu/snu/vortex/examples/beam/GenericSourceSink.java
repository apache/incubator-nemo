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
package edu.snu.vortex.examples.beam;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.Read;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.io.Write;
import org.apache.beam.sdk.io.hdfs.HDFSFileSink;
import org.apache.beam.sdk.io.hdfs.HDFSFileSource;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PDone;

/**
 * Helper class for handling source/sink in a generic way.
 * Assumes String-type Pcollections.
 */
final class GenericSourceSink {
  private GenericSourceSink() {
  }

  public static PCollection<String> read(final Pipeline pipeline,
                                         final String path) {
    if (path.startsWith("hdfs://")) {
      return pipeline.apply(Read.from(HDFSFileSource.fromText(path)));
    } else {
      return pipeline.apply(TextIO.Read.from(path));
    }
  }

  public static PDone write(final PCollection<String> dataToWrite,
                            final String path) {
    if (path.startsWith("hdfs://")) {
      return dataToWrite.apply(Write.to(HDFSFileSink.toText(path)));
    } else {
      return dataToWrite.apply(TextIO.Write.to(path));
    }
  }
}
