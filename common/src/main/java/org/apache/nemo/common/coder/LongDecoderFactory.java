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

/**
 * A {@link DecoderFactory} which is used for long.
 */
public final class LongDecoderFactory implements DecoderFactory<Long> {

  private static final LongDecoderFactory LONG_DECODER_FACTORY = new LongDecoderFactory();

  /**
   * A private constructor.
   */
  private LongDecoderFactory() {
    // do nothing.
  }

  /**
   * Static initializer of the coder.
   * @return the initializer.
   */
  public static LongDecoderFactory of() {
    return LONG_DECODER_FACTORY;
  }

  @Override
  public Decoder<Long> create(final InputStream inputStream) {
    return new LongDecoder(inputStream);
  }

  /**
   * LongDecoder.
   */
  private final class LongDecoder implements Decoder<Long> {
    private final DataInputStream inputStream;

    /**
     * Constructor.
     *
     * @param inputStream  the input stream to decode.
     */
    private LongDecoder(final InputStream inputStream) {
      // If the inputStream is closed well in upper level, it is okay to not close this stream
      // because the DataInputStream itself will not contain any extra information.
      // (when we close this stream, the input will be closed together.)
      this.inputStream = new DataInputStream(inputStream);
    }

    @Override
    public Long decode() throws IOException {
      return inputStream.readLong();
    }
  }
}
