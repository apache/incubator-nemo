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


/**
 * An interface for tracking input watermarks among multiple input streams.
 * --edge 1--&gt;
 * --edge 2--&gt;  watermarkManager --(emitWatermark)--&gt; nextOperator
 * --edge 3--&gt;
 */
public interface InputWatermarkManager {

  /**
   * This tracks the minimum input watermark among multiple input streams.
   * This method is not a Thread-safe so the caller should synchronize it
   * if multiple threads access this method concurrently.
   * Ex)
   * -- input stream1 (edge 1):  ---------- ts: 3 ------------------ts: 6
   * ^^^
   * emit ts: 4 (edge 2) watermark at this time
   * -- input stream2 (edge 2):  ----------------- ts: 4------
   * ^^^
   * emit ts: 3 (edge 1) watermark at this time
   * -- input stream3 (edge 3):  ------- ts: 5 ---------------
   *
   * @param edgeIndex incoming edge index
   * @param watermark watermark emitted from the edge
   */
  void trackAndEmitWatermarks(int edgeIndex, Watermark watermark);
}
