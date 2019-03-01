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
package org.apache.nemo.runtime.executor.data;

import org.apache.nemo.common.DecodeStreamChainer;
import org.apache.nemo.common.EncodeStreamChainer;
import org.apache.nemo.common.Serializer;
import org.apache.nemo.common.coder.DecoderFactory;
import org.apache.nemo.common.coder.EncoderFactory;
import org.apache.nemo.runtime.executor.data.streamchainer.*;
import org.apache.nemo.common.ir.edge.executionproperty.CompressionProperty;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import javax.inject.Inject;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * Mapping from RuntimeEdgeId to {@link Serializer}.
 */
public final class SerializerManager implements Serializable {
  private static final Logger LOG = LoggerFactory.getLogger(SerializerManager.class.getName());
  public final ConcurrentMap<String, Serializer> runtimeEdgeIdToSerializer = new ConcurrentHashMap<>();

  /**
   * Constructor.
   */
  @Inject
  private SerializerManager() {
  }

  /**
   * Register a encoderFactory for runtime edge.
   * This method regards that compression and decompression property are empty.
   *
   * @param runtimeEdgeId  id of the runtime edge.
   * @param encoderFactory the corresponding encoder factory.
   * @param decoderFactory the corresponding decoder factory.
   */
  public void register(final String runtimeEdgeId,
                       final EncoderFactory encoderFactory,
                       final DecoderFactory decoderFactory) {
    register(runtimeEdgeId, encoderFactory, decoderFactory, null, null);
  }

  /**
   * Register a encoderFactory for runtime edge.
   *
   * @param runtimeEdgeId         id of the runtime edge.
   * @param encoderFactory        the corresponding encoder factory.
   * @param decoderFactory        the corresponding decoder factory.
   * @param compressionProperty   compression property, or null not to enable compression
   * @param decompressionProperty decompression property, or null not to enable decompression
   */
  public void register(final String runtimeEdgeId,
                       final EncoderFactory encoderFactory,
                       final DecoderFactory decoderFactory,
                       @Nullable final CompressionProperty.Value compressionProperty,
                       @Nullable final CompressionProperty.Value decompressionProperty) {
    LOG.debug("{} edge id registering to SerializerManager", runtimeEdgeId);

    final List<EncodeStreamChainer> encodeStreamChainers = new ArrayList<>();
    final List<DecodeStreamChainer> decodeStreamChainers = new ArrayList<>();

    // Compression chain
    if (compressionProperty != null) {
      LOG.debug("Adding {} compression chain for {}",
          compressionProperty, runtimeEdgeId);
      encodeStreamChainers.add(new CompressionStreamChainer(compressionProperty));
    }
    if (decompressionProperty != null) {
      LOG.debug("Adding {} decompression chain for {}",
          decompressionProperty, runtimeEdgeId);
      decodeStreamChainers.add(new DecompressionStreamChainer(decompressionProperty));
    }

    final Serializer serializer =
        new Serializer(encoderFactory, decoderFactory, encodeStreamChainers, decodeStreamChainers);
    runtimeEdgeIdToSerializer.putIfAbsent(runtimeEdgeId, serializer);
  }

  /**
   * Return the serializer for the specified runtime edge.
   *
   * @param runtimeEdgeId id of the runtime edge.
   * @return the corresponding serializer.
   */
  public Serializer getSerializer(final String runtimeEdgeId) {
    final Serializer serializer = runtimeEdgeIdToSerializer.get(runtimeEdgeId);
    if (serializer == null) {
      throw new RuntimeException("No serializer is registered for " + runtimeEdgeId);
    }
    return serializer;
  }
}
