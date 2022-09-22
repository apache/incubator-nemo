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
package org.apache.nemo.common.ir.vertex.transform;

import org.apache.nemo.common.punctuation.Watermark;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A {@link Transform} relays input data from upstream vertex to downstream vertex promptly.
 * This transform can be used for merging input data into the {@link org.apache.nemo.common.ir.OutputCollector}.
 *
 * @param <T> input/output type.
 */
public final class StreamTransform<T> extends LatencymarkEmitTransform<T, T> {
  private static final Logger LOG = LoggerFactory.getLogger(StreamTransform.class.getName());

  /**
   * Default constructor.
   */
  public StreamTransform() {
    // Do nothing.
  }

  @Override
  public void onData(final T element) {
    getOutputCollector().emit(element);
  }

  @Override
  public void onWatermark(final Watermark watermark) {
    getOutputCollector().emitWatermark(watermark);
  }

  @Override
  public void close() {
    // Do nothing.
  }

  @Override
  public String toString() {
    final StringBuilder sb = new StringBuilder();
    sb.append(StreamTransform.class);
    sb.append(":");
    sb.append(super.toString());
    return sb.toString();
  }
}
