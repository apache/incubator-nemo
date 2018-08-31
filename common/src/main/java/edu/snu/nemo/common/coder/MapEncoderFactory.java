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

import org.apache.beam.sdk.coders.CoderException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.OutputStream;
import java.util.Iterator;
import java.util.Map;

/**
 * An EncoderFactory for Map. Reference: MapCoder in BEAM.
 * @param <K> type of the key.
 * @param <V> type of the value.
 */
public final class MapEncoderFactory<K, V> implements EncoderFactory<Map<K, V>> {
  private static final Logger LOG = LoggerFactory.getLogger(MapEncoderFactory.class.getName());
  private final EncoderFactory<K> keyEncoderFactory;
  private final EncoderFactory<V> valueEncoderFactory;

  /**
   * Private constructor of MapEncoderFactory class.
   *
   * @param keyEncoderFactory  coder for key element.
   * @param valueEncoderFactory coder for value element.
   */
  private MapEncoderFactory(final EncoderFactory<K> keyEncoderFactory,
                            final EncoderFactory<V> valueEncoderFactory) {
    this.keyEncoderFactory = keyEncoderFactory;
    this.valueEncoderFactory = valueEncoderFactory;
  }

  /**
   * static initializer of the class.
   *
   * @param keyEncoderFactory  key coder.
   * @param valueEncoderFactory value coder.
   * @param <K>          type of the key element.
   * @param <V>          type of the value element.
   * @return the new PairEncoderFactory.
   */
  public static <K, V> MapEncoderFactory<K, V> of(final EncoderFactory<K> keyEncoderFactory,
                                                  final EncoderFactory<V> valueEncoderFactory) {
    return new MapEncoderFactory<>(keyEncoderFactory, valueEncoderFactory);
  }

  @Override
  public Encoder<Map<K, V>> create(final OutputStream outputStream) throws IOException {
    return new MapEncoder<>(outputStream, keyEncoderFactory, valueEncoderFactory);
  }

  /**
   * MapEncoder.
   * @param <Key> type for the key coder.
   * @param <Value> type for the value coder.
   */
  private final class MapEncoder<Key, Value> implements Encoder<Map<Key, Value>> {
    private final Encoder<Key> keyEncoder;
    private final Encoder<Value> valueEncoder;

    /**
     * Constructor.
     *
     * @param outputStream        the output stream to store the encoded bytes.
     * @param keyEncoderFactory   the actual encoder to use for key elements.
     * @param valueEncoderFactory the actual encoder to use for value elements.
     * @throws IOException if fail to instantiate coders.
     */
    private MapEncoder(final OutputStream outputStream,
                       final EncoderFactory<Key> keyEncoderFactory,
                       final EncoderFactory<Value> valueEncoderFactory) throws IOException {
      this.keyEncoder = keyEncoderFactory.create(outputStream);
      this.valueEncoder = valueEncoderFactory.create(outputStream);
    }

    @Override
    public void encode(final Map<Key, Value> map) throws IOException {
      if (map == null) {
        throw new CoderException("cannot encode a null Map");
      }

      int size = map.size();
      if (size == 0) {
        return;
      }

      // As map size > 0, entry is guaranteed to exist before and after loop
      Iterator<Map.Entry<Key, Value>> iterator = map.entrySet().iterator();
      Map.Entry<Key, Value> entry = iterator.next();
      while (iterator.hasNext()) {
        keyEncoder.encode(entry.getKey());
        valueEncoder.encode(entry.getValue());
        entry = iterator.next();
      }

      keyEncoder.encode(entry.getKey());
      valueEncoder.encode(entry.getValue());
    }
  }
}
