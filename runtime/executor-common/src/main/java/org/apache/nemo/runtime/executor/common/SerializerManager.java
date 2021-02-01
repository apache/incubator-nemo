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
package org.apache.nemo.runtime.executor.common;
import org.apache.nemo.common.coder.DecoderFactory;
import org.apache.nemo.common.coder.EncoderFactory;
import org.apache.nemo.common.ir.edge.executionproperty.CompressionProperty;
import javax.annotation.Nullable;

import java.io.Serializable;

/**
 * Mapping from RuntimeEdgeId to {@link Serializer}.
 */
public interface SerializerManager extends Serializable {

  /**
   * Register a encoderFactory for runtime edge.
   * This method regards that compression and decompression property are empty.
   *
   * @param runtimeEdgeId  id of the runtime edge.
   * @param encoderFactory the corresponding encoder factory.
   * @param decoderFactory the corresponding decoder factory.
   */
  void register(final String runtimeEdgeId,
                       final EncoderFactory encoderFactory,
                       final DecoderFactory decoderFactory);

  /**
   * Register a encoderFactory for runtime edge.
   *
   * @param runtimeEdgeId         id of the runtime edge.
   * @param encoderFactory        the corresponding encoder factory.
   * @param decoderFactory        the corresponding decoder factory.
   * @param compressionProperty   compression property, or null not to enable compression
   * @param decompressionProperty decompression property, or null not to enable decompression
   */
  void register(final String runtimeEdgeId,
                       final EncoderFactory encoderFactory,
                       final DecoderFactory decoderFactory,
                       @Nullable final CompressionProperty.Value compressionProperty,
                       @Nullable final CompressionProperty.Value decompressionProperty);

  /**
   * Return the serializer for the specified runtime edge.
   *
   * @param runtimeEdgeId id of the runtime edge.
   * @return the corresponding serializer.
   */
  Serializer getSerializer(final String runtimeEdgeId);
}
