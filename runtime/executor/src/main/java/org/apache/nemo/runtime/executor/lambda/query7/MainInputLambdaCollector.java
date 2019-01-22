/*
t
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
package org.apache.nemo.runtime.executor.lambda.query7;

import org.apache.beam.sdk.util.WindowedValue;
import org.apache.commons.lang.SerializationUtils;
import org.apache.nemo.common.coder.EncoderFactory;
import org.apache.nemo.common.ir.OutputCollector;
import org.apache.nemo.common.ir.vertex.IRVertex;
import org.apache.nemo.common.ir.vertex.OperatorVertex;
import org.apache.nemo.common.punctuation.Watermark;
import org.apache.nemo.runtime.common.plan.StageEdge;
import org.apache.nemo.runtime.executor.data.SerializerManager;
import org.apache.nemo.runtime.executor.datatransfer.NextIntraTaskOperatorInfo;
import org.apache.nemo.runtime.executor.datatransfer.OperatorVertexOutputCollector;
import org.apache.nemo.runtime.executor.lambda.LambdaWarmer;
import org.apache.nemo.runtime.executor.datatransfer.NemoEventDecoderFactory;
import org.apache.nemo.runtime.executor.datatransfer.NemoEventEncoderFactory;
import org.apache.nemo.runtime.executor.lambda.StorageObjectFactory;
import org.apache.nemo.runtime.lambda.LambdaDecoderFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.*;

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

  private final Map<String, Info> windowAndInfoMap = new HashMap<>();
  private final Map<String, Integer> windowAndPartitionMap = new HashMap<>();

  private final EncoderFactory<O> encoderFactory;
  private EncoderFactory.Encoder<O> encoder;
  private final byte[] encodedDecoderFactory;
  private final StorageObjectFactory storageObjectFactory;

  private final ExecutorService executorService = Executors.newCachedThreadPool();
  private final long period = 10000;

  private final List<NextIntraTaskOperatorInfo> internalMainOutputs;
  /**
   * Constructor of the output collector.
   * @param irVertex the ir vertex that emits the output
   */
  public MainInputLambdaCollector(
    final IRVertex irVertex,
    final List<NextIntraTaskOperatorInfo> internalMainOutputs,
    final List<StageEdge> outgoingEdges,
    final SerializerManager serializerManager,
    final StorageObjectFactory storageObjectFactory) {
    this.irVertex = irVertex;
    this.storageObjectFactory = storageObjectFactory;
    this.internalMainOutputs = internalMainOutputs;

    this.encoderFactory = ((NemoEventEncoderFactory) serializerManager.getSerializer(outgoingEdges.get(0).getId())
      .getEncoderFactory()).getValueEncoderFactory();
    final LambdaDecoderFactory decoderFactory = new LambdaDecoderFactory(
      ((NemoEventDecoderFactory) serializerManager.getSerializer(outgoingEdges.get(0).getId())
      .getDecoderFactory()).getValueDecoderFactory());
    this.encodedDecoderFactory = SerializationUtils.serialize(decoderFactory);


    /*
    if (LambdaWarmer.TICKET.getAndIncrement() == 0) {
      this.warmer = new LambdaWarmer();
      warmer.warmup();
    } else {
      this.warmer = null;
    }
    */
  }

  private void checkAndFlush(final String key) {
    final Info info = windowAndInfoMap.get(key);
    info.cnt += 1;

    //LOG.info("Info {}, count: {}", info.fname, info.cnt);

    //if (info.cnt >= 10000 || info.accessTime - prevAccessTime >= 2000) {
    final long currtime = System.currentTimeMillis();
    if (System.currentTimeMillis() - info.triggerTime >= period) {
      info.triggerTime = currtime;
      LOG.info("Trigger");

      windowAndInfoMap.put(key, null);
      // flush
      executorService.execute(() -> {
        LOG.info("Close {}", info.storageObject);
        info.storageObject.close();
      });
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

  private void toLambda(final O output) {
    //LOG.info("{} emits {}", irVertex.getId(), output);

    // buffer data
    final WindowedValue wv = (WindowedValue) output;
    for (WindowedValue wvv : (Iterable<WindowedValue>) wv.explodeWindows()) {
      final String fileName =
        wvv.getWindows().iterator().next().toString() + "__" + this.hashCode();

      //LOG.info("Vertex 6 output: {} ******** {}", fileName, wvv);
      Info info = windowAndInfoMap.get(fileName);

      if (info == null) {

        if (windowAndPartitionMap.get(fileName) == null) {
          windowAndPartitionMap.put(fileName, 0);
        }

        final int partition = windowAndPartitionMap.get(fileName);

        /*
        if (warmer != null) {
          if (partition == 1) {
            // warm up
            //warmer.warmup();
          }
        }
        */


        info = new Info(storageObjectFactory.newInstance(
          wvv.getWindows().iterator().next().toString() ,
          "__" + this.hashCode(), partition, encodedDecoderFactory, encoderFactory));
        windowAndInfoMap.put(fileName, info);
        windowAndPartitionMap.put(fileName, windowAndPartitionMap.get(fileName) + 1);
      }

      info.storageObject.encode(wvv);
      //LOG.info("Write count {}, of {}", info.dbos.getCount(), info.fname);

      // time to flush?
      checkAndFlush(fileName);
    }

    // send to serverless
    //return;
  }

  @Override
  public <T> void emit(final String dstVertexId, final T output) {
    // do nothing
  }


  private void emit(final OperatorVertex vertex, final O output) {

    final String vertexId = irVertex.getId();
    vertex.getTransform().onData(output);
  }

  @Override
  public void emit(O output) {
    // emit internal
    for (final NextIntraTaskOperatorInfo internalVertex : internalMainOutputs) {
      emit(internalVertex.getNextOperator(), output);
    }

    // emit lambda
    toLambda(output);
  }

  @Override
  public void emitWatermark(final Watermark watermark) {

  }

  final class Info {
    public int cnt = 0;
    public long triggerTime = System.currentTimeMillis();
    public final StorageObjectFactory.StorageObject storageObject;

    Info(StorageObjectFactory.StorageObject storageObject) {
      this.storageObject = storageObject;
    }

    @Override
    public String toString() {
      return "{cnt: " + cnt + ", obj: " + storageObject + "}";
    }
  }


}
