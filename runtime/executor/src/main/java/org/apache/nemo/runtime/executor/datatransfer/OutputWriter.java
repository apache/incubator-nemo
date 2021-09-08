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

import java.util.Map;
import java.util.Optional;

/**
 * Represents the output data transfer from a task.
 */
public interface OutputWriter {
  /**
   * Writes output element depending on the communication pattern of the edge.
   *
   * @param element the element to write.
   */
  void write(Object element);

  /**
   * Writes watermarks to all edges.
   *
   * @param watermark watermark
   */
  void writeWatermark(Watermark watermark);

  /**
   * @return the total written bytes.
   */
  Optional<Long> getWrittenBytes();

  /**
   * @return the map of hashed key to partition size.
   */
  Optional<Map<Integer, Long>> getPartitionSizeMap();

  void close();
}
