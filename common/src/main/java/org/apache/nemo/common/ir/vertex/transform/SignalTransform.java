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

import org.apache.nemo.common.ir.OutputCollector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class evokes run-time pass when there's no need to transfer any run-time information.
 */
public final class SignalTransform extends NoWatermarkEmitTransform<Void, Void> {
  private static final Logger LOG = LoggerFactory.getLogger(SignalTransform.class.getName());
  private transient Void elementHolder;
  private transient OutputCollector<Void> outputCollector;

  @Override
  public void prepare(final Context context, final OutputCollector<Void> oc) {
    this.outputCollector = oc;
  }

  @Override
  public void onData(final Void element) {
    elementHolder = element;
  }

  @Override
  public void close() {
    outputCollector.emit(elementHolder);
  }

  @Override
  public String toString() {
    final StringBuilder sb = new StringBuilder();
    sb.append(SignalTransform.class);
    sb.append(":");
    sb.append(super.toString());
    return sb.toString();
  }
}
