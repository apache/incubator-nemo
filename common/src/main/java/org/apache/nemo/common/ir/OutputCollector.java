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
package org.apache.nemo.common.ir;

import org.apache.nemo.common.punctuation.Watermark;

import java.io.Serializable;

/**
 * Interface through which Transform emits outputs.
 * This is to be implemented in the runtime with
 * runtime-specific distributed data movement and storage mechanisms.
 * @param <O> output type.
 */
public interface OutputCollector<O> extends Serializable {
  /**
   * Single-destination emit.
   * @param output value.
   */
  void emit(O output);

  /**
   * Emit watermark to downstream vertices.
   * @param watermark watermark
   */
  void emitWatermark(Watermark watermark);

  /**
   * Multi-destination emit.
   * Currently unused, but might come in handy
   * for operations like multi-output map.
   * @param dstVertexId destination vertex id.
   * @param output value.
   * @param <T> output type.
   */
  <T> void emit(String dstVertexId, T output);
}
