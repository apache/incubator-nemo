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
import java.io.InputStream;
import java.io.Serializable;

/**
 * A decoder factory object which generates decoders that decode byte streams into values of type {@code T}.
 * To avoid generating instance-based coder such as Spark serializer for every decoding,
 * user need to instantiate a decoder instance and use it.
 *
 * @param <T> element type.
 */
// TODO #120: Separate EOFException from Decoder Failures
public interface OffloadingDecoder<T> extends Serializable {

  /**
   * Get a decoder instance.
   *
   * @param inputStream the input stream to decode.
   * @return the decoder instance.
   * @throws IOException if fail to get the instance.
   */
  T decode(InputStream inputStream) throws IOException;
}
