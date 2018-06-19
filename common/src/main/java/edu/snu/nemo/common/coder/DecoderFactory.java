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
import java.io.Serializable;

/**
 * A decoder factory object which generates decoders that decode values of type {@code T} into byte streams.
 * To avoid to generate instance-based coder such as Spark serializer for every decoding,
 * user need to instantiate a decoder instance and use it.
 *
 * @param <T> element type.
 */
public interface DecoderFactory<T> extends Serializable {

  /**
   * Get a decoder instance.
   *
   * @param inputStream the input stream to decode.
   * @return the decoder instance.
   * @throws IOException if fail to get the instance.
   */
  Decoder<T> create(InputStream inputStream) throws IOException;

  /**
   * Interface of Decoder.
   *
   * @param <T> element type.
   */
  interface Decoder<T> extends Serializable {

    /**
     * Decodes the a value from the given input stream.
     * It have to be able to decode the given stream consequently by calling this method repeatedly.
     * Because there are many elements in the input stream, the stream should not be closed.
     *
     * @return the decoded element
     * @throws IOException if fail to decode
     */
    T decode() throws IOException;
  }

  /**
   * Dummy coder factory.
   */
  DecoderFactory DUMMY_DECODER_FACTORY = new DummyDecoderFactory();

  /**
   * Dummy coder factory implementation which is not supposed to be used.
   */
  final class DummyDecoderFactory implements DecoderFactory {

    private final Decoder dummyDecoder = new DummyDecoder();

    /**
     * DummyDecoder.
     */
    private final class DummyDecoder implements Decoder {

      @Override
      public Object decode() {
        throw new RuntimeException("DummyDecoder is not supposed to be used.");
      }
    }

    @Override
    public Decoder create(final InputStream inputStream) {
      return dummyDecoder;
    }

    @Override
    public String toString() {
      return "DUMMY_DECODER_FACTORY";
    }
  }
}
