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

import com.amazonaws.services.lambda.AWSLambda;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.PutObjectRequest;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
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
import java.util.List;

import static org.apache.nemo.runtime.executor.datatransfer.AWSUtils.S3_BUCKET_NAME;

public final class SideInputLambdaCollector<O> implements OutputCollector<O> {
  private static final Logger LOG = LoggerFactory.getLogger(SideInputLambdaCollector.class.getName());

  private final IRVertex irVertex;

  // TODO: remove
  private final AmazonS3 amazonS3;
  private final AWSLambda awsLambda;
  private EncoderFactory<O> encoderFactory;
  private EncoderFactory.Encoder<O> encoder;
  private OutputStream outputStream;

  int cnt = 0;


  /**
   * Constructor of the output collector.
   * @param irVertex the ir vertex that emits the output
   */
  public SideInputLambdaCollector(
    final IRVertex irVertex,
    final List<StageEdge> outgoingEdges,
    final SerializerManager serializerManager) {
    this.irVertex = irVertex;
    this.encoderFactory = ((NemoEventEncoderFactory) serializerManager.getSerializer(outgoingEdges.get(0).getId())
      .getEncoderFactory()).getValueEncoderFactory();
    this.amazonS3 = AWSUtils.AWS_S3;
    this.awsLambda = AWSUtils.AWS_LAMBDA;
  }

  private EncoderFactory.Encoder createEncoder(final String fileName) {
    try {
      outputStream = new FileOutputStream(fileName);
      return encoderFactory.create(outputStream);
    } catch (IOException e) {
      e.printStackTrace();
      throw new RuntimeException(e);
    }
  }

  private void closeEncoder() {
    try {
      outputStream.close();
      encoder = null;
    } catch (IOException e) {
      e.printStackTrace();
      throw new RuntimeException();
    }
  }

  @Override
  public void emit(final O output) {
        // CreateViewTransform (side input)
    // send to serverless
    final WindowedValue wv = (WindowedValue) output;
    final BoundedWindow window = (BoundedWindow) wv.getWindows().iterator().next();
    LOG.info("Vertex20 Window: {} ********** {}", window, window.maxTimestamp().toString());
    final String fileName = window.toString();
    try {
      if (encoder == null) {
        encoder = createEncoder(fileName);
      }
      encoder.encode((O) wv);

      closeEncoder();

      final File file = new File(fileName);

      LOG.info("Start to send sideinput data to S3");
      final PutObjectRequest putObjectRequest =
        new PutObjectRequest(S3_BUCKET_NAME + "/sideinput", file.getName(), file);
      amazonS3.putObject(putObjectRequest);
      file.delete();
      LOG.info("End of send sideinput to S3");

      // Trigger lambdas

    } catch (IOException e) {
      e.printStackTrace();
      throw new RuntimeException(e);
    }
    return;
  }

  @Override
  public <T> void emit(final String dstVertexId, final T output) {
    // do nothing
  }

  @Override
  public void emitWatermark(final Watermark watermark) {
    // do nothing
  }
}
