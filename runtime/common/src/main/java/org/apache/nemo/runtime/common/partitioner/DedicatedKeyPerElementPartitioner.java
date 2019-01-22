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
package org.apache.nemo.runtime.common.partitioner;

import org.apache.nemo.common.ir.vertex.transform.StreamTransform;

/**
 * An implementation of {@link Partitioner} which assigns a dedicated key per an output data from a task.
 * WARNING: Because this partitioner assigns a dedicated key per element, it should be used under specific circumstances
 * that the number of output element is not that many. For example, every output element of
 * {@link StreamTransform} inserted by large shuffle optimization is always
 * a partition. In this case, assigning a key for each element can be useful.
 */
@DedicatedKeyPerElement
public final class DedicatedKeyPerElementPartitioner implements Partitioner<Integer> {
  private int key;

  /**
   * Constructor.
   */
  public DedicatedKeyPerElementPartitioner() {
    // TODO #288: DedicatedKeyPerElementPartitioner should not always return 0
    key = 0;
  }

  @Override
  public Integer partition(final Object element) {
    return key++;
  }
}
