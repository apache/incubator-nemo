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
import java.io.InputStream;

/**
 * A kv decoder factory object which generates kv decoders that decode byte streams to values of type {@code T}.
 * To avoid generating instance-based coder such as Spark serializer for every decoding,
 * user need to explicitly instantiate an kv decoder instance and use it.
 *
 * @param <T> element type.
 */
public interface KVDecoderFactory<T> extends DecoderFactory {
  @Override
  KVDecoder<T> create(InputStream inputStream) throws IOException;
  DecoderFactory getKeyDecoderFactory();

  /**
   * Interface of the Decoder.
   *
   * @param <T> element type.
   */
  interface KVDecoder<T> extends Decoder<T> {
    /**
     * Decodes the given value onto the specified input stream.
     * It has to be able to decode the given stream consequently by calling this method repeatedly.
     * Because the user can want to keep a single input stream and continuously concatenate elements,
     * the input stream should not be closed.
     *
     * @throws IOException if fail to decode
     */
    T decode() throws IOException;
  }

  /**
   * Dummy kv decoder factory.
   */
  KVDecoderFactory DUMMY_KVDECODER_FACTORY = new DummyKVDecoderFactory();

  /**
   * Dummy kv decoder factory implementation which is not supposed to be used.
   */
  final class DummyKVDecoderFactory implements KVDecoderFactory {
    private final DecoderFactory keyDecoderFactory = null;
    private final KVDecoder dummyKVDecoder = new DummyKVDecoder();

    /**
     * DummyKVDecoder.
     */
    private final class DummyKVDecoder implements KVDecoder {
      @Override
      public Object decode() {
        throw new RuntimeException("DummyKVDecoder is not supposed to be used.");
      }
    }

    @Override
    public KVDecoder create(final InputStream inputStream) {
      return dummyKVDecoder;
    }

    @Override
    public DecoderFactory getKeyDecoderFactory() {
      return keyDecoderFactory;
    }

    @Override
    public String toString() {
      return "DUMMY_KVDECODER_FACTORY";
    }
  }
}
