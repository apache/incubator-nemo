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
package org.apache.nemo.runtime.executor.datatransfer;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.PutObjectRequest;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.nemo.common.coder.EncoderFactory;
import org.apache.nemo.common.ir.OutputCollector;
import org.apache.nemo.common.ir.vertex.IRVertex;
import org.apache.nemo.common.punctuation.Watermark;
import org.apache.nemo.runtime.common.plan.StageEdge;
import org.apache.nemo.runtime.executor.data.SerializerManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import static org.apache.nemo.runtime.executor.datatransfer.AWSUtils.S3_BUCKET_NAME;

/**
 * OutputCollector implementation.
 * This emits four types of outputs
 * 1) internal main outputs: this output becomes the input of internal Transforms
 * 2) internal additional outputs: this additional output becomes the input of internal Transforms
 * 3) external main outputs: this external output is emitted to OutputWriter
 * 4) external additional outputs: this external output is emitted to OutputWriter
 *
 * @param <O> output type.
 */
public final class MainInputLambdaCollector<O> implements OutputCollector<O> {
  private static final Logger LOG = LoggerFactory.getLogger(MainInputLambdaCollector.class.getName());

  private final IRVertex irVertex;

  private final AmazonS3 amazonS3;
  private final Map<String, Info> windowAndInfoMap = new HashMap<>();
  private final Map<String, Integer> windowAndPartitionMap = new HashMap<>();
  private final EncoderFactory<O> encoderFactory;
  private EncoderFactory.Encoder<O> encoder;

  /**
   * Constructor of the output collector.
   * @param irVertex the ir vertex that emits the output
   */
  public MainInputLambdaCollector(
    final IRVertex irVertex,
    final List<StageEdge> outgoingEdges,
    final SerializerManager serializerManager) {
    this.irVertex = irVertex;

    this.encoderFactory = ((NemoEventEncoderFactory) serializerManager.getSerializer(outgoingEdges.get(0).getId())
      .getEncoderFactory()).getValueEncoderFactory();
    this.amazonS3 = AmazonS3ClientBuilder.standard().build();
  }

  private void checkAndFlush(final String fileName) {
    final Info info = windowAndInfoMap.get(fileName);
    info.cnt += 1;
    info.accessTime = System.currentTimeMillis();

    if (info.cnt >= 10) {
      // flush
      info.close();
      windowAndInfoMap.put(fileName, null);
    }

    final long currTime = System.currentTimeMillis();

    /*
    for (final String key : windowAndInfoMap.keySet()) {
      final Info info1 = windowAndInfoMap.get(key);
      if (info1 != null) {
        if (currTime - info1.accessTime >= 1000) {
          info1.close();
          windowAndInfoMap.put(key, null);
        }
      }
    }
    */
  }

  @Override
  public void emit(final O output) {
    //LOG.info("{} emits {}", irVertex.getId(), output);

    // buffer data
    final WindowedValue wv = (WindowedValue) output;
    for (WindowedValue wvv : (Iterable<WindowedValue>) wv.explodeWindows()) {
      final String fileName =
        wvv.getWindows().iterator().next().toString() + "__" + this.hashCode();

      LOG.info("Vertex 6 output: {} ******** {}", fileName, wvv);
      Info info = windowAndInfoMap.get(fileName);

      if (info == null) {

        if (windowAndPartitionMap.get(fileName) == null) {
          windowAndPartitionMap.put(fileName, 0);
        }

        info = new Info(fileName, windowAndPartitionMap.get(fileName));
        windowAndInfoMap.put(fileName, info);
        windowAndPartitionMap.put(fileName, windowAndPartitionMap.get(fileName) + 1);
      }

      try {
        info.encoder.encode(wv);
      } catch (IOException e) {
        e.printStackTrace();
        throw new RuntimeException(e);
      }

      // time to flush?
      checkAndFlush(fileName);
    }

    // send to serverless
    //return;
  }

  @Override
  public <T> void emit(final String dstVertexId, final T output) {
  }

  @Override
  public void emitWatermark(final Watermark watermark) {

  }

  final class Info {
    public final EncoderFactory.Encoder encoder;
    public final OutputStream outputStream;
    public int cnt;
    public long accessTime;
    public final String fname;

    public Info(final String fileName, final int partition) {
      this.cnt = 0;
      this.accessTime = System.currentTimeMillis();
      this.fname = fileName + "-" + partition;
      try {
        this.outputStream = new FileOutputStream(fname);
        this.encoder = encoderFactory.create(outputStream);
      } catch (IOException e) {
        e.printStackTrace();
        throw new RuntimeException(e);
      }
    }

    public void close() {
      try {
        outputStream.close();
        final File file = new File(fname);
        LOG.info("Start to send main input data to S3 {}", file.getName());
        final PutObjectRequest putObjectRequest =
          new PutObjectRequest(S3_BUCKET_NAME + "/maininput", file.getName(), file);
        amazonS3.putObject(putObjectRequest);
        file.delete();
        LOG.info("End of send main input to S3 {}", file.getName());

      } catch (IOException e) {
        e.printStackTrace();
        throw new RuntimeException(e);
      }
    }
  }


}
