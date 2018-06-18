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
import java.io.Serializable;

/**
 * A coder object encodes values of type {@code T} into byte streams.
 * To avoid to generate instance-based coder such as Spark serializer for every encoding,
 * user need to explicitly instantiate an encoder instance and use it.
 *
 * @param <T> element type.
 */
public interface Encoder<T> extends Serializable {

  /**
   * Get an instance of this encoder.
   *
   * @param outputStream the stream on which encoded bytes are written
   * @return the encoder instance.
   * @throws IOException if fail to get the instance.
   */
  EncoderInstance<T> getEncoderInstance(OutputStream outputStream) throws IOException;

  /**
   * Interface of EncoderInstance.
   *
   * @param <T> element type.
   */
  interface EncoderInstance<T> {

    /**
     * Encodes the given value onto the specified output stream.
     * It have to be able to encode the given stream consequently by calling this method repeatedly.
     * Because the user can want to keep a single output stream and continuously concatenate elements,
     * the output stream should not be closed.
     *
     * @param element   the element to be encoded
     * @throws IOException if fail to encode
     */
    void encode(T element) throws IOException;
  }

  /**
   * Dummy encoder.
   */
  Encoder DUMMY_ENCODER = new DummyEncoder();

  /**
   * Dummy encoder implementation which is not supposed to be used.
   */
  final class DummyEncoder implements Encoder {

    /**
     * DummyEncoderInstance.
     */
    private final class DummyEncoderInstance implements EncoderInstance {

      @Override
      public void encode(final Object element) {
        throw new RuntimeException("DummyEncoderInstance is not supposed to be used.");
      }
    }

    @Override
    public EncoderInstance getEncoderInstance(final OutputStream outputStream) {
      return new DummyEncoderInstance();
    }

    @Override
    public String toString() {
      return "DUMMY_ENCODER";
    }
  }
}
