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
 * A decoder object decodes values of type {@code T} into byte streams.
 * To avoid to generate instance-based coder such as Spark serializer for every decoding,
 * user need to instantiate a decoder instance and use it.
 *
 * @param <T> element type.
 */
public interface Decoder<T> extends Serializable {

  /**
   * Get an instance of this decoder.
   *
   * @param inputStream the input stream to decode.
   * @return the decoder instance.
   * @throws IOException if fail to get the instance.
   */
  DecoderInstance<T> getDecoderInstance(InputStream inputStream) throws IOException;

  /**
   * Interface of DecoderInstance.
   *
   * @param <T> element type.
   */
  interface DecoderInstance<T> {

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
   * Dummy coder.
   */
  Decoder DUMMY_DECODER = new DummyDecoder();

  /**
   * Dummy coder implementation which is not supposed to be used.
   */
  final class DummyDecoder implements Decoder {

    /**
     * DummyDecoderInstance.
     */
    private final class DummyDecoderInstance implements DecoderInstance {

      @Override
      public Object decode() {
        throw new RuntimeException("DummyDecoderInstance is not supposed to be used.");
      }
    }

    @Override
    public DecoderInstance getDecoderInstance(final InputStream inputStream) {
      return new DummyDecoderInstance();
    }

    @Override
    public String toString() {
      return "DUMMY_ENCODER";
    }
  }
}
