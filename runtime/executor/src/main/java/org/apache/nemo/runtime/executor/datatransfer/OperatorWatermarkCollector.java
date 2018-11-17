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
 * This class is used for collecting watermarks for an OperatorVertex.
 * InputWatermarkManager emits watermarks to this class.
 */
public final class OperatorWatermarkCollector implements OutputCollector {
  private static final Logger LOG = LoggerFactory.getLogger(OperatorWatermarkCollector.class.getName());

  private final OperatorVertex operatorVertex;

  public OperatorWatermarkCollector(final OperatorVertex operatorVertex) {
    this.operatorVertex = operatorVertex;
  }

  @Override
  public void emit(final Object output) {
    throw new IllegalStateException("Should not be called");
  }

  @Override
  public void emitWatermark(final Watermark watermark) {
    LOG.info(operatorVertex.getId());
    operatorVertex.getTransform().onWatermark(watermark);
  }

  @Override
  public void emit(final String dstVertexId, final Object output) {
    throw new IllegalStateException("Should not be called");
  }
}
