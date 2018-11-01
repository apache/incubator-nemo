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

import org.apache.nemo.common.ir.vertex.OperatorVertex;
import org.apache.nemo.common.punctuation.Watermark;


/**
 * This is a special implementation for single input data stream for optimization.
 */
public final class SingleInputWatermarkManager implements InputWatermarkManager {

  private final OperatorVertex nextOperator;

  public SingleInputWatermarkManager(final OperatorVertex nextOperator) {
    this.nextOperator = nextOperator;
  }

  /**
   * This just forwards watermarks to the next operator because it has one data stream.
   * @param edgeIndex edge index
   * @param watermark watermark
   */
  @Override
  public void trackAndEmitWatermarks(final int edgeIndex,
                                     final Watermark watermark) {
    nextOperator.getTransform().onWatermark(watermark);
  }
}
