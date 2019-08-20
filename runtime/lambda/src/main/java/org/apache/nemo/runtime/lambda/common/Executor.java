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
package org.apache.nemo.runtime.lambda.common;

import org.apache.commons.lang3.SerializationUtils;
import org.apache.commons.lang3.concurrent.BasicThreadFactory;
import org.apache.nemo.common.coder.BytesDecoderFactory;
import org.apache.nemo.common.coder.BytesEncoderFactory;
import org.apache.nemo.common.coder.DecoderFactory;
import org.apache.nemo.common.coder.EncoderFactory;
import org.apache.nemo.common.dag.DAG;
import org.apache.nemo.common.ir.edge.executionproperty.CompressionProperty;
import org.apache.nemo.common.ir.edge.executionproperty.DecoderProperty;
import org.apache.nemo.common.ir.edge.executionproperty.DecompressionProperty;
import org.apache.nemo.common.ir.edge.executionproperty.EncoderProperty;
import org.apache.nemo.common.ir.vertex.IRVertex;
import org.apache.nemo.runtime.common.plan.RuntimeEdge;
import org.apache.nemo.runtime.common.plan.Task;
import org.apache.nemo.runtime.executor.datatransfer.NemoEventDecoderFactory;
import org.apache.nemo.runtime.executor.datatransfer.NemoEventEncoderFactory;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Executor.
 */
public final class Executor {
  private final String executorId;

  /**
   * To be used for a thread pool to execute tasks.
   */
  private final ExecutorService executorService;

  /**
   * In charge of this executor's intermediate data transfer.
   */
  private final SerializerManager serializerManager;

  public Executor() {
    this.executorId = "LambdaExecutor";
    this.executorService = Executors.newCachedThreadPool(new BasicThreadFactory.Builder()
      .namingPattern("TaskExecutor thread-%d")
      .build());
    this.serializerManager = new SerializerManager();
  }

   public void onTaskReceived(final Task task) {
     // should be a blocking function
     System.out.println("Executor [{}] received Task [{}] to execute." +
       new Object[]{executorId, task.getTaskId()});
     executorService.execute(() -> launchTask(task));
   }

  /**
   * Launches the Task, and keeps track of the execution state with taskStateManager.
   *
   * @param task to launch.
   */
  private void launchTask(final Task task) {
    System.out.println("Launch task: {}" + task.getTaskId());
    try {
      final DAG<IRVertex, RuntimeEdge<IRVertex>> irDag =
        SerializationUtils.deserialize(task.getSerializedIRDag());

      task.getTaskIncomingEdges().forEach(e -> serializerManager.register(e.getId(),
        getEncoderFactory(e.getPropertyValue(EncoderProperty.class).get()),
        getDecoderFactory(e.getPropertyValue(DecoderProperty.class).get()),
        e.getPropertyValue(CompressionProperty.class).orElse(null),
        e.getPropertyValue(DecompressionProperty.class).orElse(null)));
      task.getTaskOutgoingEdges().forEach(e -> serializerManager.register(e.getId(),
        getEncoderFactory(e.getPropertyValue(EncoderProperty.class).get()),
        getDecoderFactory(e.getPropertyValue(DecoderProperty.class).get()),
        e.getPropertyValue(CompressionProperty.class).orElse(null),
        e.getPropertyValue(DecompressionProperty.class).orElse(null)));
      irDag.getVertices().forEach(v -> {
        irDag.getOutgoingEdgesOf(v).forEach(e -> serializerManager.register(e.getId(),
          getEncoderFactory(e.getPropertyValue(EncoderProperty.class).get()),
          getDecoderFactory(e.getPropertyValue(DecoderProperty.class).get()),
          e.getPropertyValue(CompressionProperty.class).orElse(null),
          e.getPropertyValue(DecompressionProperty.class).orElse(null)));
      });

      new TaskExecutor(task, irDag).execute();
    } catch (final Exception e) {
      e.printStackTrace();
    }
  }

  /**
   * This wraps the encoder with NemoEventEncoder.
   * If the encoder is BytesEncoderFactory, we do not wrap the encoder.
   * TODO #276: Add NoCoder property value in Encoder/DecoderProperty
   *
   * @param encoderFactory encoder factory
   * @return wrapped encoder
   */
  private EncoderFactory getEncoderFactory(final EncoderFactory encoderFactory) {
    if (encoderFactory instanceof BytesEncoderFactory) {
      return encoderFactory;
    } else {
      return new NemoEventEncoderFactory(encoderFactory);
    }
  }

  /**
   * This wraps the encoder with NemoEventDecoder.
   * If the decoder is BytesDecoderFactory, we do not wrap the decoder.
   * TODO #276: Add NoCoder property value in Encoder/DecoderProperty
   *
   * @param decoderFactory decoder factory
   * @return wrapped decoder
   */
  private DecoderFactory getDecoderFactory(final DecoderFactory decoderFactory) {
    if (decoderFactory instanceof BytesDecoderFactory) {
      return decoderFactory;
    } else {
      return new NemoEventDecoderFactory(decoderFactory);
    }
  }

/*  public void terminate() {
    try {
      metricMessageSender.close();
    } catch (final UnknownFailureCauseException e) {
      throw new UnknownFailureCauseException(
        new Exception("Closing MetricManagerWorker failed in executor " + executorId));
    }
  }*/
}
