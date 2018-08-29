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

import java.io.*;
import java.util.Collections;
import java.util.Map;

/**
 * An EncoderFactory for Map. Reference: MapCoder in BEAM.
 * @param <Key> type of the key.
 * @param <Value> type of the value.
 */
public final class MapDecoderFactory<Key, Value> implements DecoderFactory<Map<Key, Value>> {
  private final DecoderFactory<Key> keyDecoderFactory;
  private final DecoderFactory<Value> valueDecoderFactory;

  /**
   * Private constructor of MapEncoderFactory class.
   *
   * @param keyDecoderFactory  coder for right element.
   * @param valueDecoderFactory coder for right element.
   */
  private MapDecoderFactory(final DecoderFactory<Key> keyDecoderFactory,
                            final DecoderFactory<Value> valueDecoderFactory) {
    this.keyDecoderFactory = keyDecoderFactory;
    this.valueDecoderFactory = valueDecoderFactory;
  }

  /**
   * static initializer of the class.
   *
   * @param keyDecoderFactory  left coder.
   * @param valueDecoderFactory right coder.
   * @param <K>          type of the left element.
   * @param <V>          type of the right element.
   * @return the new PairEncoderFactory.
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
   * PairEncoder.
   * @param <Key> type for the left coder.
   * @param <Value> type for the right coder.
   */
  private final class MapDecoder<Key, Value> implements Decoder<Map<Key, Value>> {
    private final Decoder<Key> keyDecoder;
    private final Decoder<Value> valueDecoder;
    private final InputStream inputStream;
  
    /**
     * Constructor.
     *
     * @param inputStream        the output stream to store the encoded bytes.
     * @param keyDecoderFactory   the actual encoder to use for left elements.
     * @param valueDecoderFactory the actual encoder to use for right elements.
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
