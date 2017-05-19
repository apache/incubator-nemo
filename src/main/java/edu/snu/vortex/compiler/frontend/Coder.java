/*
 * Copyright (C) 2017 Seoul National University
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
package edu.snu.vortex.compiler.frontend;

import java.io.InputStream;
import java.io.OutputStream;
import java.io.Serializable;

/**
 * A {@link Coder Coder&lt;T&gt;} object encodes or decodes values of type {@code T} into byte streams.
 *
 * @param <T> type of values being encoded or decoded
 */
public interface Coder<T> extends Serializable {
  /**
   * Encodes the given value onto the specified output stream.
   *
   * @param value the value to be encoded
   * @param outStream the stream on which encoded bytes are written
   */
  void encode(T value, OutputStream outStream);

  /**
   * Decodes the a value from the given input stream.
   *
   * @param inStream the stream from which bytes are read
   * @return the decoded value
   */
  T decode(InputStream inStream);

  /**
   * Dummy coder.
   */
  Coder DUMMY_CODER = new DummyCoder();

  /**
   * Dummy coder implementation which is not supposed to be used.
   */
  final class DummyCoder implements Coder<Object> {

    @Override
    public void encode(final Object value, final OutputStream outStream) {
      throw new RuntimeException("DummyCoder is not supposed to be used.");
    }

    @Override
    public Object decode(final InputStream inStream) {
      throw new RuntimeException("DummyCoder is not supposed to be used.");
    }

    @Override
    public String toString() {
      return "DUMMY_CODER";
    }
  }
}
