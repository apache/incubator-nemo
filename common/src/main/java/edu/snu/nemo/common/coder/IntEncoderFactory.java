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
 * A {@link EncoderFactory} which is used for an integer.
 */
public final class IntEncoderFactory implements EncoderFactory<Integer> {

  private static final IntEncoderFactory INT_ENCODER_FACTORY = new IntEncoderFactory();

  /**
   * A private constructor.
   */
  private IntEncoderFactory() {
    // do nothing.
  }

  /**
   * Static initializer of the coder.
   */
  public static IntEncoderFactory of() {
    return INT_ENCODER_FACTORY;
  }

  @Override
  public Encoder<Integer> create(final OutputStream outputStream) {
    return new IntEncoder(outputStream);
  }

  /**
   * IntEncoder.
   */
  private final class IntEncoder implements Encoder<Integer> {

    private final DataOutputStream outputStream;

    /**
     * Constructor.
     *
     * @param outputStream the output stream to store the encoded bytes.
     */
    private IntEncoder(final OutputStream outputStream) {
      // If the outputStream is closed well in upper level, it is okay to not close this stream
      // because the DataOutputStream itself will not contain any extra information.
      // (when we close this stream, the output will be closed together.)
      this.outputStream = new DataOutputStream(outputStream);
    }

    @Override
    public void encode(final Integer value) throws IOException {
      outputStream.writeInt(value);
    }
  }
}
