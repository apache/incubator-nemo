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

import java.io.IOException;
import java.io.OutputStream;

/**
 * A kv encoder factory object which generates kv encoders that encode values of type {@code T} into byte streams.
 * To avoid to generate instance-based coder such as Spark serializer for every encoding,
 * user need to explicitly instantiate an kv encoder instance and use it.
 *
 * @param <T> element type.
 */
public interface KVEncoderFactory<T> extends EncoderFactory {
  @Override
  KVEncoder<T> create(OutputStream outputStream) throws IOException;
  EncoderFactory getKeyEncoderFactory();

  /**
   * Interface of the Encoder.
   *
   * @param <T> element type.
   */
  interface KVEncoder<T> extends Encoder<T> {
    /**
     * Encodes the given value onto the specified output stream.
     * It has to be able to encode the given stream consequently by calling this method repeatedly.
     * Because the user may want to keep a single output stream and continuously concatenate elements,
     * the output stream should not be closed.
     *
     * @param element the element to be encoded
     * @throws IOException if fail to encode
     */
    void encode(T element) throws IOException;
  }

  /**
   * Dummy kv encoder factory.
   */
  KVEncoderFactory DUMMY_KVENCODER_FACTORY = new DummyKVEncoderFactory();

  /**
   * Dummy kv encoder factory implementation which is not supposed to be used.
   */
  final class DummyKVEncoderFactory implements KVEncoderFactory {
    private final EncoderFactory keyEncoderFactory = DUMMY_ENCODER_FACTORY;
    private final KVEncoder dummyKVEncoder = new DummyKVEncoder();

    /**
     * DummyKVEncoder.
     */
    private final class DummyKVEncoder implements KVEncoder {
      @Override
      public void encode(final Object element) {
        throw new RuntimeException("DummyKVEncoder is not supposed to be used.");
      }
    }

    @Override
    public KVEncoder create(final OutputStream outputStream) {
      return dummyKVEncoder;
    }

    @Override
    public EncoderFactory getKeyEncoderFactory() {
      return keyEncoderFactory;
    }

    @Override
    public String toString() {
      return "DUMMY_KVENCODER_FACTORY";
    }
  }
}
