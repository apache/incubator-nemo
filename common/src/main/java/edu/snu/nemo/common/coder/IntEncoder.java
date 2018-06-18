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
 * A {@link Encoder} which is used for an integer.
 */
public final class IntEncoder implements Encoder<Integer> {

  /**
   * A private constructor.
   */
  private IntEncoder() {
    // do nothing.
  }

  /**
   * Static initializer of the coder.
   */
  public static IntEncoder of() {
    return new IntEncoder();
  }

  @Override
  public EncoderInstance<Integer> getEncoderInstance(final OutputStream outputStream) {
    return new IntEncoderInstance(outputStream);
  }

  /**
   * IntEncoderInstance.
   */
  private final class IntEncoderInstance implements EncoderInstance<Integer> {

    private final OutputStream outputStream;

    /**
     * Constructor.
     *
     * @param outputStream the output stream to store the encoded bytes.
     */
    private IntEncoderInstance(final OutputStream outputStream) {
      this.outputStream = outputStream;
    }

    @Override
    public void encode(final Integer value) throws IOException {
      final DataOutputStream dataOutputStream = new DataOutputStream(outputStream);
      dataOutputStream.writeInt(value);
    }
  }
}
