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
package org.apache.nemo.runtime.executor.common.datatransfer;

import org.apache.nemo.common.punctuation.Watermark;

import java.io.Flushable;
import java.util.*;
import java.util.concurrent.Future;

/**
 * Represents the output data transfer from a task.
 */
public interface OutputWriter {
  /**
   * Writes output element depending on the communication pattern of the edge.
   *
   * @param element the element to write.
   */
  void write(final Object element);

  /**
   * Writes watermarks to all edges.
   * @param watermark watermark
   */
  void writeWatermark(final Watermark watermark);

  /**
   * @return the total written bytes.
   */
  Optional<Long> getWrittenBytes();

  void close();

  Future<Boolean> stop(final String taskId);

  void restart(final String taskId);
}