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
package edu.snu.onyx.common.coder;

import edu.snu.onyx.compiler.ir.Element;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.Serializable;

/**
 * A {@link Coder Coder&lt;T&gt;} object encodes or decodes values of type {@code T} into byte streams.
 *
 * @param <Data> data type.
 * @param <Key> key type.
 * @param <Value> value type.
 */
public interface Coder<Data, Key, Value> extends Serializable {
  /**
   * Encodes the given value onto the specified output stream.
   * It have to be able to encode the given stream consequently by calling this method repeatedly.
   *
   * @param value the value to be encoded
   * @param outStream the stream on which encoded bytes are written
   * @throws IOException if fail to encode.
   */
  void encode(Element<Data, Key, Value> value, OutputStream outStream) throws IOException;

  /**
   * Decodes the a value from the given input stream.
   * It have to be able to decode the given stream consequently by calling this method repeatedly.
   *
   * @param inStream the stream from which bytes are read
   * @return the decoded value
   * @throws IOException if fail to decode.
   */
  Element<Data, Key, Value> decode(InputStream inStream) throws IOException;

  /**
   * Dummy coder.
   */
  Coder DUMMY_CODER = new DummyCoder();

  /**
   * Dummy coder implementation which is not supposed to be used.
   */
  final class DummyCoder implements Coder {

    @Override
    public void encode(final Element value, final OutputStream outStream) {
      throw new RuntimeException("DummyCoder is not supposed to be used.");
    }

    @Override
    public Element decode(final InputStream inStream) {
      throw new RuntimeException("DummyCoder is not supposed to be used.");
    }

    @Override
    public String toString() {
      return "DUMMY_CODER";
    }
  }
}
