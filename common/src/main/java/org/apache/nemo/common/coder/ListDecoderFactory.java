/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.nemo.common.coder;

import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;

/**
 * A {@link DecoderFactory} which is used for an integer.
 */
public final class ListDecoderFactory<T> implements DecoderFactory<List<T>> {

  private final DecoderFactory<T> valueDecoder;
  /**
   * A private constructor.
   */
  private ListDecoderFactory(final DecoderFactory<T> value) {
    this.valueDecoder = value;
  }

  /**
   * Static initializer of the coder.
   * @return the initializer.
   */
  public static ListDecoderFactory of(final DecoderFactory valueDecoder) {
    return new ListDecoderFactory(valueDecoder);
  }

  @Override
  public Decoder<List<T>> create(final InputStream inputStream) {
    return new ListDecoder(inputStream);
  }

  @Override
  public String toString() {
    return "ListDecoderFacory{}";
  }

  /**
   * IntDecoder.
   */
  private final class ListDecoder implements Decoder<List<T>> {

    private final DataInputStream inputStream;
    private final Decoder<T> decoder;

    /**
     * Constructor.
     *
     * @param inputStream  the input stream to decode.
     */
    private ListDecoder(final InputStream inputStream) {
      // If the inputStream is closed well in upper level, it is okay to not close this stream
      // because the DataInputStream itself will not contain any extra information.
      // (when we close this stream, the input will be closed together.)
      this.inputStream = new DataInputStream(inputStream);
      try {
        this.decoder = valueDecoder.create(inputStream);
      } catch (IOException e) {
        e.printStackTrace();
        throw new RuntimeException(e);
      }
    }

    @Override
    public List<T> decode() throws IOException {
      final int size = inputStream.readInt();
      final List<T> array = new ArrayList<>(size);
      for (int i = 0; i < size; i++) {
        array.add(decoder.decode());
      }
      return array;
    }
  }
}
