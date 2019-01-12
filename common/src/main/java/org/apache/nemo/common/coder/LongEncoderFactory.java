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

import java.io.DataOutputStream;
import java.io.IOException;
import java.io.OutputStream;

/**
 * A {@link EncoderFactory} which is used for long.
 */
public final class LongEncoderFactory implements EncoderFactory<Long> {

  private static final LongEncoderFactory LONG_ENCODER_FACTORY = new LongEncoderFactory();

  /**
   * A private constructor.
   */
  private LongEncoderFactory() {
    // do nothing.
  }

  /**
   * Static initializer of the coder.
   * @return the initializer.
   */
  public static LongEncoderFactory of() {
    return LONG_ENCODER_FACTORY;
  }

  @Override
  public Encoder<Long> create(final OutputStream outputStream) {
    return new LongEncoder(outputStream);
  }

  /**
   * LongEncoder.
   */
  private final class LongEncoder implements Encoder<Long> {

    private final DataOutputStream outputStream;

    /**
     * Constructor.
     *
     * @param outputStream the output stream to store the encoded bytes.
     */
    private LongEncoder(final OutputStream outputStream) {
      // If the outputStream is closed well in upper level, it is okay to not close this stream
      // because the DataOutputStream itself will not contain any extra information.
      // (when we close this stream, the output will be closed together.)
      this.outputStream = new DataOutputStream(outputStream);
    }

    @Override
    public void encode(final Long value) throws IOException {
      outputStream.writeLong(value);
    }
  }
}
