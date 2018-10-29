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
package org.apache.nemo.compiler.frontend.spark.transform;

import org.apache.nemo.common.ir.OutputCollector;
import org.apache.nemo.common.ir.vertex.transform.Transform;
import org.apache.commons.lang3.SerializationUtils;

import java.util.ArrayList;
import java.util.Base64;

/**
 * Collect transform.
 * @param <T> type of data to collect.
 */
public final class CollectTransform<T> implements Transform<T, T> {
  private final ArrayList<T> list;
  private Context ctxt;

  /**
   * Constructor.
   */
  public CollectTransform() {
    this.list = new ArrayList<>();
  }

  @Override
  public void prepare(final Context context, final OutputCollector<T> oc) {
    this.ctxt = context;
  }

  @Override
  public void onData(final T element) {
    list.add(element);
  }

  @Override
  public void close() {
    ctxt.setSerializedData(Base64.getEncoder().encodeToString(SerializationUtils.serialize(list)));
  }
}
