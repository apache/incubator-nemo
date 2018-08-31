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
package edu.snu.nemo.common.coder;

import com.google.common.collect.Maps;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.Collections;
import java.util.Map;

/**
 * An DecoderFactory for Map. Reference: MapCoder in BEAM.
 * @param <Key> type of the key.
 * @param <Value> type of the value.
 */
public final class MapDecoderFactory<Key, Value> implements DecoderFactory<Map<Key, Value>> {
  private static final Logger LOG = LoggerFactory.getLogger(MapDecoderFactory.class);

  private final DecoderFactory<Key> keyDecoderFactory;
  private final DecoderFactory<Value> valueDecoderFactory;

  /**
   * Private constructor of MapDecoderFactory class.
   *
   * @param keyDecoderFactory  coder for key element.
   * @param valueDecoderFactory coder for value element.
   */
  private MapDecoderFactory(final DecoderFactory<Key> keyDecoderFactory,
                            final DecoderFactory<Value> valueDecoderFactory) {
    this.keyDecoderFactory = keyDecoderFactory;
    this.valueDecoderFactory = valueDecoderFactory;
  }

  /**
   * static initializer of the class.
   *
   * @param keyDecoderFactory  key coder.
   * @param valueDecoderFactory value coder.
   * @param <K>          type of the key element.
   * @param <V>          type of the value element.
   * @return the new MapDecoderFactory.
   */
  public static <K, V> MapDecoderFactory<K, V> of(final DecoderFactory<K> keyDecoderFactory,
                                                  final DecoderFactory<V> valueDecoderFactory) {
    return new MapDecoderFactory<>(keyDecoderFactory, valueDecoderFactory);
  }

  @Override
  public Decoder<Map<Key, Value>> create(final InputStream inputStream) throws IOException {
    return new MapDecoder<>(inputStream, keyDecoderFactory, valueDecoderFactory);
  }

  /**
   * MapDecoder.
   * @param <Key> type for the key coder.
   * @param <Value> type for the value coder.
   */
  private final class MapDecoder<Key, Value> implements Decoder<Map<Key, Value>> {
    private final Decoder<Key> keyDecoder;
    private final Decoder<Value> valueDecoder;
    private final InputStream inputStream;

    /**
     * Constructor.
     *
     * @param inputStream        the output stream to store the encoded bytes.
     * @param keyDecoderFactory   the actual decoder to use for key elements.
     * @param valueDecoderFactory the actual decoder to use for value elements.
     * @throws IOException if fail to instantiate coders.
     */
    private MapDecoder(final InputStream inputStream,
                       final DecoderFactory<Key> keyDecoderFactory,
                       final DecoderFactory<Value> valueDecoderFactory) throws IOException {
      this.keyDecoder = keyDecoderFactory.create(inputStream);
      this.valueDecoder = valueDecoderFactory.create(inputStream);
      this.inputStream = inputStream;
    }

    @Override
    public Map<Key, Value> decode() throws IOException {
      DataInputStream dataInStream = new DataInputStream(inputStream);
      int size = dataInStream.readInt();
      if (size == 0) {
        return Collections.emptyMap();
      }

      Map<Key, Value> decodedMap = Maps.newHashMapWithExpectedSize(size);
      for (int i = 0; i < size - 1; ++i) {
        Key key = keyDecoder.decode();
        Value value = valueDecoder.decode();
        decodedMap.put(key, value);
      }

      Key key = keyDecoder.decode();
      Value value = valueDecoder.decode();
      decodedMap.put(key, value);
      return decodedMap;
    }
  }
}
