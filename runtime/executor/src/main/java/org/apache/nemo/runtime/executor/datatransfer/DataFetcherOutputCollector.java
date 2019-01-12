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

import org.apache.nemo.common.ir.OutputCollector;
import org.apache.nemo.common.ir.vertex.OperatorVertex;
import org.apache.nemo.common.punctuation.Watermark;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This collector receives data from DataFetcher and forwards it to the next operator.
 * @param <O> output type.
 */
public final class DataFetcherOutputCollector<O> implements OutputCollector<O> {
  private static final Logger LOG = LoggerFactory.getLogger(DataFetcherOutputCollector.class.getName());
  private final OperatorVertex nextOperatorVertex;
  private final int edgeIndex;
  private final InputWatermarkManager watermarkManager;

  /**
   * It forwards output to the next operator.
   * @param nextOperatorVertex next operator to emit data and watermark
   * @param edgeIndex edge index
   * @param watermarkManager watermark manager
   */
  public DataFetcherOutputCollector(final OperatorVertex nextOperatorVertex,
                                    final int edgeIndex,
                                    final InputWatermarkManager watermarkManager) {
    this.nextOperatorVertex = nextOperatorVertex;
    this.edgeIndex = edgeIndex;
    this.watermarkManager = watermarkManager;
  }

  @Override
  public void emit(final O output) {
    nextOperatorVertex.getTransform().onData(output);
  }

  @Override
  public void emitWatermark(final Watermark watermark) {
    watermarkManager.trackAndEmitWatermarks(edgeIndex, watermark);
  }

  @Override
  public <T> void emit(final String dstVertexId, final T output) {
    throw new RuntimeException("No additional output tag in DataFetcherOutputCollector");
  }
}
