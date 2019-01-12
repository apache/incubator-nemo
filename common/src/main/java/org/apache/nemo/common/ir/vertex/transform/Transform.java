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
import org.apache.nemo.common.punctuation.Watermark;

import java.io.Serializable;
import java.util.Optional;

/**
 * Interface for specifying 'What' to do with data.
 * It is to be implemented in the compiler frontend, possibly for every operator in a dataflow language.
 * 'How' and 'When' to do with its input/output data are up to the runtime.
 * @param <I> input type.
 * @param <O> output type.
 */
public interface Transform<I, O> extends Serializable {
  /**
   * Prepare the transform.
   * @param context of the transform.
   * @param outputCollector that collects outputs.
   */
  void prepare(Context context, OutputCollector<O> outputCollector);

  /**
   * On data received.
   * @param element data received.
   */
  void onData(I element);

  /**
   * On watermark received.
   * This method should be called for the minimum watermark among input streams (input watermark).
   * Transform may emit collected data after receiving watermarks.
   * @param watermark watermark
   */
  void onWatermark(Watermark watermark);

  /**
   * Close the transform.
   */
  void close();

  /**
   * Context of the transform.
   */
  interface Context extends Serializable {
    /**
     * @param id of the variable to get.
     * @return the broadcast variable.
     */
    Object getBroadcastVariable(Serializable id);

    /**
     * Put serialized data to send to the executor.
     * @param serializedData the serialized data.
     */
    void setSerializedData(String serializedData);

    /**
     * Retrieve the serialized data on the executor.
     * @return the serialized data.
     */
    Optional<String> getSerializedData();
  }
}
