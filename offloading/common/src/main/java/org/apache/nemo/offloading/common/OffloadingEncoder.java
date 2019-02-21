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
package org.apache.nemo.offloading.common;

import java.io.IOException;
import java.io.OutputStream;
import java.io.Serializable;

/**
 * A encoder factory object which generates encoders that encode values of type {@code T} into byte streams.
 * To avoid to generate instance-based coder such as Spark serializer for every encoding,
 * user need to explicitly instantiate an encoder instance and use it.
 *
 * @param <T> element type.
 */
public interface OffloadingEncoder<T> extends Serializable {

  /**
   * Get an encoder instance.
   *
   * @param outputStream the stream on which encoded bytes are written
   * @return the encoder instance.
   * @throws IOException if fail to get the instance.
   */
  void encode(T element,
              OutputStream outputStream) throws IOException;
}
