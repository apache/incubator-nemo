/*
 * Copyvalue (C) 2018 Seoul National University
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

import org.apache.beam.sdk.values.KV;

import java.io.IOException;
import java.io.OutputStream;

/**
 * An EncoderFactory for {@link KV}. Reference: KvCoder in BEAM.
 * @param <K> type for the key coder.
 * @param <V> type for the value coder.
 */
public final class KVEncoderFactory<K, V> implements EncoderFactory<KV<K, V>> {
  private final EncoderFactory<K> keyEncoderFactory;
  private final EncoderFactory<V> valueEncoderFactory;

  /**
   * Private constructor of KVEncoderFactory class.
   *
   * @param keyEncoderFactory  coder for key element.
   * @param valueEncoderFactory coder for value element.
   */
  private KVEncoderFactory(final EncoderFactory<K> keyEncoderFactory,
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
   * @return the new KVEncoderFactory.
   */
  public static <K, V> KVEncoderFactory<K, V> of(final EncoderFactory<K> keyEncoderFactory,
                                                 final EncoderFactory<V> valueEncoderFactory) {
    return new KVEncoderFactory<>(keyEncoderFactory, valueEncoderFactory);
  }

  @Override
  public Encoder<KV<K, V>> create(final OutputStream outputStream) throws IOException {
    return new KVEncoder<>(outputStream, keyEncoderFactory, valueEncoderFactory);
  }

  /**
   * KVEncoder.
   * @param <Key> type for the key coder.
   * @param <Value> type for the value coder.
   */
  private final class KVEncoder<Key, Value> implements Encoder<KV<Key, Value>> {

    private final Encoder<Key> keyEncoder;
    private final Encoder<Value> valueEncoder;

    /**
     * Constructor.
     *
     * @param outputStream the output stream to store the encoded bytes.
     * @param keyEncoderFactory  the actual encoder to use for key elements.
     * @param valueEncoderFactory the actual encoder to use for value elements.
     * @throws IOException if fail to instantiate coders.
     */
    private KVEncoder(final OutputStream outputStream,
                        final EncoderFactory<Key> keyEncoderFactory,
                        final EncoderFactory<Value> valueEncoderFactory) throws IOException {
      this.keyEncoder = keyEncoderFactory.create(outputStream);
      this.valueEncoder = valueEncoderFactory.create(outputStream);
    }

    @Override
    public void encode(final KV<Key, Value> kv) throws IOException {
      if (kv == null) {
        throw new IOException("cannot encode a null KV");
      }
      keyEncoder.encode(kv.getKey());
      valueEncoder.encode(kv.getValue());
    }
  }
}
