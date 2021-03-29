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

import org.apache.nemo.common.Util;
import org.apache.nemo.common.ir.OutputCollector;
import org.apache.nemo.common.punctuation.Watermark;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Random;

/**
 * A {@link Transform} relays input data from upstream vertex to downstream vertex promptly.
 * This transform can be used for merging input data into the {@link OutputCollector}.
 * @param <T> input/output type.
 */
public final class CRTransform<T> implements Transform<T, T> {
  private OutputCollector<T> outputCollector;
  private static final Logger LOG = LoggerFactory.getLogger(CRTransform.class.getName());

  private double percent = 0.0;
  private boolean toParital = false;

  private final Random random = new Random();
  /**
   * Default constructor.
   */
  public CRTransform() {
    // Do nothing.
  }

  @Override
  public void prepare(final Context context, final OutputCollector<T> oc) {
    this.outputCollector = oc;
  }

  @Override
  public void onData(final T element) {
    if (toParital) {
      if (random.nextDouble() < percent) {
        outputCollector.emit(Util.PARTIAL_RR_TAG, element);
      } else {
        outputCollector.emit(element);
      }

    } else {
      outputCollector.emit(element);
    }
  }

  public void setCRRouting(final double percent, final boolean toPartial) {
    this.percent = percent;
    this.toParital = toPartial;
  }


  @Override
  public void onWatermark(final Watermark watermark) {
    outputCollector.emitWatermark(watermark);
  }

  @Override
  public void close() {
    // Do nothing.
  }

  @Override
  public String toString() {
    final StringBuilder sb = new StringBuilder();
    sb.append(CRTransform.class);
    sb.append(":");
    sb.append(super.toString());
    return sb.toString();
  }
}
