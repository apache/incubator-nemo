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

import java.io.*;

/**
 * A {@link EncoderFactory} which is used for an array of bytes.
 */
public final class BytesEncoderFactory implements EncoderFactory<byte[]> {

  private static final BytesEncoderFactory BYTES_ENCODER_FACTORY = new BytesEncoderFactory();

  /**
   * A private constructor.
   */
  private BytesEncoderFactory() {
    // do nothing.
  }

  /**
   * Static initializer of the encoder.
   */
  public static BytesEncoderFactory of() {
    return BYTES_ENCODER_FACTORY;
  }

  @Override
  public Encoder<byte[]> create(final OutputStream outputStream) {
    return new BytesEncoder(outputStream);
  }

  /**
   * BytesEncoder.
   */
  private final class BytesEncoder implements Encoder<byte[]> {

    private final OutputStream outputStream;

    /**
     * Constructor.
     *
     * @param outputStream the output stream to store the encoded bytes.
     */
    private BytesEncoder(final OutputStream outputStream) {
      this.outputStream = outputStream;
    }

    @Override
    public void encode(final byte[] value) throws IOException {
      // Write the byte[] as is.
      // Because this interface use the length of byte[] element,
      // the element must not have any padding bytes.
      outputStream.write(value);
    }
  }
}
