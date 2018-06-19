/*
 * Copyright (C) 2018 Seoul National University
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package edu.snu.nemo.runtime.executor.data;

import edu.snu.nemo.common.coder.DecoderFactory;
import edu.snu.nemo.common.coder.EncoderFactory;
import edu.snu.nemo.runtime.executor.data.streamchainer.*;
import edu.snu.nemo.common.ir.edge.executionproperty.CompressionProperty;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import javax.inject.Inject;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * Mapping from RuntimeEdgeId to {@link Serializer}.
 */
public final class SerializerManager {
  private static final Logger LOG = LoggerFactory.getLogger(SerializerManager.class.getName());
  private final ConcurrentMap<String, Serializer> runtimeEdgeIdToSerializer = new ConcurrentHashMap<>();

  /**
   * Constructor.
   */
  @Inject
  public SerializerManager() {
  }

  /**
   * Register a encoderFactory for runtime edge.
   * This method regards that compression & decompression property are empty.
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
