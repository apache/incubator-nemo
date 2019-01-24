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
package org.apache.nemo.runtime.executor.offloading.lambda.query7;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.services.lambda.AWSLambda;
import com.amazonaws.services.lambda.AWSLambdaClientBuilder;
import com.amazonaws.services.lambda.model.InvokeRequest;
import com.amazonaws.services.lambda.model.InvokeResult;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.*;
import org.apache.commons.lang.SerializationUtils;
import org.apache.nemo.common.coder.DecoderFactory;
import org.apache.nemo.common.coder.EncoderFactory;
import org.apache.nemo.runtime.executor.data.SerializerManager;
import org.apache.nemo.runtime.executor.datatransfer.AWSUtils;
import org.apache.nemo.runtime.executor.datatransfer.NemoEventDecoderFactory;
import org.apache.nemo.runtime.executor.datatransfer.NemoEventEncoderFactory;
import org.apache.nemo.runtime.executor.offloading.SideInputProcessor;
import org.apache.nemo.common.lambda.LambdaDecoderFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.stream.Collectors;

import static org.apache.nemo.runtime.executor.datatransfer.AWSUtils.S3_BUCKET_NAME;

public final class S3SideInputProcessor<O> implements SideInputProcessor<O> {
  private static final Logger LOG = LoggerFactory.getLogger(S3SideInputProcessor.class.getName());

  private final AmazonS3 amazonS3;
  private final AWSLambda awsLambda;
  private final EncoderFactory<O> encoderFactory;
  private EncoderFactory.Encoder<O> encoder = null;
  private final DecoderFactory decoderFactory;
  private final byte[] encodedDecoderFactory;
  private OutputStream outputStream;

  private final ExecutorService executorService = Executors.newCachedThreadPool();

  public S3SideInputProcessor(final SerializerManager serializerManager,
                              final String edgeId) {
    this.amazonS3 = AmazonS3ClientBuilder.standard().build();
    this.awsLambda = AWSLambdaClientBuilder.standard().withClientConfiguration(
      new ClientConfiguration().withMaxConnections(150)).build();
    this.encoderFactory = ((NemoEventEncoderFactory) serializerManager.getSerializer(edgeId)
      .getEncoderFactory()).getValueEncoderFactory();
    this.decoderFactory = new LambdaDecoderFactory(
      ((NemoEventDecoderFactory) serializerManager.getSerializer(edgeId)
        .getDecoderFactory()).getValueDecoderFactory());
    this.encodedDecoderFactory = SerializationUtils.serialize(decoderFactory);
  }

  @Override
  public Object processSideAndMainInput(final O output,
                                      final String sideInputKey) {

    try {
      if (encoder == null) {
        encoder = createEncoder(sideInputKey);
        outputStream.write(encodedDecoderFactory);
      }
      encoder.encode((O) output);

      closeEncoder();

      final File file = new File(sideInputKey);

      //LOG.info("Start to send sideinput {}", file.getName());

      final PutObjectRequest putObjectRequest =
        new PutObjectRequest(S3_BUCKET_NAME + "/sideinput", file.getName(), file);
      amazonS3.putObject(putObjectRequest);

      // Get main input list
      final ListObjectsV2Request req =
        new ListObjectsV2Request().withBucketName(S3_BUCKET_NAME).withPrefix("maininput/" + file.getName());
      final ListObjectsV2Result result = amazonS3.listObjectsV2(req);
      LOG.info("Request sideinput lambda: {}", result.getObjectSummaries().size());
      final List<Future<InvokeResult>> futures = new ArrayList<>(result.getObjectSummaries().size());
      final List<DeleteObjectsRequest.KeyVersion> keys = new ArrayList<>(result.getObjectSummaries().size());

      for (final S3ObjectSummary objectSummary : result.getObjectSummaries()) {
        //System.out.printf(" - %s (size: %d)\n", objectSummary.getKey(), objectSummary.getSize());
        keys.add(new DeleteObjectsRequest.KeyVersion(objectSummary.getKey()));

        futures.add(executorService.submit(() -> {
          // Trigger lambdas
          final InvokeRequest request = new InvokeRequest()
            .withFunctionName(AWSUtils.SIDEINPUT_LAMBDA_NAME)
            .withPayload(String.format("{\"sideInput\":\"sideinput/%s\", \"mainInput\":\"%s\"}",
              sideInputKey, objectSummary.getKey()));

          return awsLambda.invoke(request);
          //LOG.info("End of Request sideinput lambda");
          //index = (index + 1) % awsLambdaAsyncs.size();
        }));
      }

      final List<Object> results = futures.stream().map(future -> {
        try {
          return future.get().getPayload().toString();
        } catch (InterruptedException e) {
          e.printStackTrace();
          throw new RuntimeException(e);
        } catch (ExecutionException e) {
          e.printStackTrace();
          throw new RuntimeException(e);
        }
      }).collect(Collectors.toList());

      file.delete();
      return results;
      // remove files
      //final DeleteObjectsRequest deleteObjectsRequest = new DeleteObjectsRequest(S3_BUCKET_NAME);
      //deleteObjectsRequest.setKeys(keys);
      //amazonS3.deleteObjects(deleteObjectsRequest);
      //System.out.println("Delete objects");
    } catch (IOException e) {
      e.printStackTrace();
      throw new RuntimeException(e);
    }
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
}
