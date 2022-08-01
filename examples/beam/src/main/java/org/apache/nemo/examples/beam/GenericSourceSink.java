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
package org.apache.nemo.examples.beam;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.io.hadoop.format.HadoopFormatIO;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PDone;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.nemo.common.exception.DataSourceException;

import java.io.IOException;
import java.util.UUID;

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
   *
   * @param pipeline beam pipeline
   * @param path     path to read
   * @return returns the read value
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
      final HadoopFormatIO.Read<Long, String> read = HadoopFormatIO.<Long, String>read()
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
   * NEMO-365: This method could later be replaced using the HadoopFormatIO class.
   *
   * @param dataToWrite data to write
   * @param path        path to write data
   * @return returns {@link PDone}
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
   *
   * @param path path to check
   * @return boolean value indicating whether the path is HDFS path or not
   */
  public static boolean isHDFSPath(final String path) {
    return path.startsWith("hdfs://") || path.startsWith("s3a://") || path.startsWith("file://");
  }
}

/**
 * Write output to HDFS according to the parallelism.
 */
final class HDFSWrite extends DoFn<String, Void> {
  private final String path;
  private transient Path fileName;
  private transient FileSystem fileSystem;
  private transient FSDataOutputStream outputStream;

  /**
   * Constructor.
   *
   * @param path HDFS path
   */
  HDFSWrite(final String path) {
    this.path = path;
  }

  /**
   * Writes to exactly one file.
   * (The number of total output files are determined according to the parallelism.)
   * i.e. if parallelism is 2, then there are total 2 output files.
   * TODO #273: Our custom HDFSWrite should implement WriteOperation
   */
  @Setup
  public void setup() {
    // Creating a side-effect in Setup is discouraged, but we do it anyways for now as we're extending DoFn.
    fileName = new Path(path + UUID.randomUUID().toString());
    try {
      fileSystem = fileName.getFileSystem(new JobConf());
      outputStream = fileSystem.create(fileName, false);
    } catch (IOException e) {
      throw new DataSourceException(new Exception("File system setup failed " + e));
    }
  }

  /**
   * process element.
   *
   * @param c context {@link ProcessContext}
   * @throws Exception exception.
   */
  @ProcessElement
  public void processElement(final ProcessContext c) throws DataSourceException, IOException {
    try {
      outputStream.writeBytes(c.element() + "\n");
    } catch (Exception e) {
      outputStream.close();
      fileSystem.delete(fileName, true);
      throw new DataSourceException(new Exception("Processing data from source failed " + e));
    }
  }

  /**
   * Teardown.
   *
   * @throws IOException output stream exception
   */
  @Teardown
  public void tearDown() throws IOException {
    outputStream.close();
  }
}
