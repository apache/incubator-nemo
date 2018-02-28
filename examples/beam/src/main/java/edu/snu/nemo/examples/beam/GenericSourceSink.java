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
package edu.snu.nemo.examples.beam;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.*;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.io.hadoop.inputformat.HadoopInputFormatIO;
import org.apache.beam.sdk.values.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.fs.FileSystem;

import java.io.*;
import java.util.*;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.JobConf;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Helper class for handling source/sink in a generic way.
 * Assumes String-type PCollections.
 */
final class GenericSourceSink {
  /**
   * Default Constructor.
   */
  private GenericSourceSink() {
  }

  /**
   * Read data.
   * @param pipeline  beam pipeline
   * @param path      path to read
   * @return          returns the read value
   */
  public static PCollection<String> read(final Pipeline pipeline,
                                         final String path) {
    if (isHDFSPath(path)) {
      final Configuration hadoopConf = new Configuration(true);
      hadoopConf.set("mapreduce.input.fileinputformat.inputdir", path);
      hadoopConf.setClass("mapreduce.job.inputformat.class", TextInputFormat.class, InputFormat.class);
      hadoopConf.setClass("key.class", LongWritable.class, Object.class);
      hadoopConf.setClass("value.class", Text.class, Object.class);

      // Without translations, Beam internally does some weird cloning
      final HadoopInputFormatIO.Read<Long, String> read = HadoopInputFormatIO.<Long, String>read()
          .withConfiguration(hadoopConf)
          .withKeyTranslation(new SimpleFunction<LongWritable, Long>() {
            @Override
            public Long apply(final LongWritable longWritable) {
              return longWritable.get();
            }
          })
          .withValueTranslation(new SimpleFunction<Text, String>() {
            @Override
            public String apply(final Text text) {
              return text.toString();
            }
          });
      return pipeline.apply(read).apply(MapElements.into(TypeDescriptor.of(String.class)).via(KV::getValue));
    } else {
      return pipeline.apply(TextIO.read().from(path));
    }
  }

  /**
   * Write data.
   * @param dataToWrite data to write
   * @param path        path to write data
   * @return            returns {@link PDone}
   */
  public static PDone write(final PCollection<String> dataToWrite,
                            final String path) {
    if (isHDFSPath(path)) {
      dataToWrite.apply(ParDo.of(new HDFSWrite(path)));
      return PDone.in(dataToWrite.getPipeline());
    } else {
      return dataToWrite.apply(TextIO.write().to(path));
    }
  }

  /**
   * Check if given path is HDFS path.
   * @param path path to check
   * @return     boolean value indicating whether the path is HDFS path or not
   */
  private static boolean isHDFSPath(final String path) {
    return path.startsWith("hdfs://") || path.startsWith("s3a://") || path.startsWith("file://");
  }
}

/**
 * Write output to HDFS according to the parallelism.
 */
final class HDFSWrite extends DoFn<String, Void> {
  private static final Logger LOG = LoggerFactory.getLogger(HDFSWrite.class.getName());

  private final String path;
  private Path fileName;
  private FileSystem fileSystem;
  private FSDataOutputStream outputStream;

  /**
   * Constructor.
   *
   * @param path    HDFS path
   */
  HDFSWrite(final String path) {
    this.path = path;
  }

  /**
   * Start bundle.
   * The number of output files are determined according to the parallelism.
   * i.e. if parallelism is 2, then there are total 2 output files.
   * Each output file is written as a bundle.
   * @param c      bundle context {@link StartBundleContext}
   */
  @StartBundle
  public void startBundle(final StartBundleContext c) {
    fileName = new Path(path + UUID.randomUUID().toString());
    try {
      fileSystem = fileName.getFileSystem(new JobConf());
      outputStream = fileSystem.create(fileName, false);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * process element.
   * @param c          context {@link ProcessContext}
   * @throws Exception exception.
   */
  @ProcessElement
  public void processElement(final ProcessContext c) throws Exception {
    try {
      outputStream.writeBytes(c.element() + "\n");
    } catch (Exception e) {
      outputStream.close();
      fileSystem.delete(fileName, true);
      throw new RuntimeException(e);
    }
  }

  /**
   * finish bundle.
   * @param c             context
   * @throws IOException  output stream exception
   */
  @FinishBundle
  public void finishBundle(final FinishBundleContext c) throws IOException {
    outputStream.close();
  }
}
