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

import org.apache.nemo.common.punctuation.Watermark;

import java.io.Serializable;

/**
 * This contains a watermark and the src task index.
 * It is used for transferring the watermark between tasks.
 */
public final class WatermarkWithIndex implements Serializable {
  private final Watermark watermark;
  private final int index;

  public WatermarkWithIndex(final Watermark watermark, final int index) {
    this.watermark = watermark;
    this.index = index;
  }

  public Watermark getWatermark() {
    return watermark;
  }

  public int getIndex() {
    return index;
  }

  @Override
  public String toString() {
    final StringBuilder sb = new StringBuilder();
    sb.append(watermark);
    sb.append(" from ");
    sb.append(index);
    return sb.toString();
  }
}
