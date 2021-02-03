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
import java.util.List;

/**
 * A {@link EncoderFactory} which is used for an integer.
 */
public final class ListEncoderFactory<T> implements EncoderFactory<List<T>> {

  private final EncoderFactory<T> valueEncoder;
  /**
   * A private constructor.
   */
  private ListEncoderFactory(final EncoderFactory<T> value) {
    this.valueEncoder = value;
  }

  /**
   * Static initializer of the coder.
   * @return the initializer.
   */
  public static <T> ListEncoderFactory<T> of(final EncoderFactory<T> valueEncoder) {
    return new ListEncoderFactory<>(valueEncoder);
  }

  @Override
  public Encoder<List<T>> create(final OutputStream outputStream) {
    return new ListEncoder(outputStream);
  }

  @Override
  public String toString() {
    return "ListEncoderFactory{}";
  }

  /**
   * IntEncoder.
   */
  private final class ListEncoder implements Encoder<List<T>> {

    private final DataOutputStream outputStream;
    private final Encoder<T> encoder;

    /**
     * Constructor.
     *
     * @param outputStream the output stream to store the encoded bytes.
     */
    private ListEncoder(final OutputStream outputStream) {
      // If the outputStream is closed well in upper level, it is okay to not close this stream
      // because the DataOutputStream itself will not contain any extra information.
      // (when we close this stream, the output will be closed together.)
      this.outputStream = new DataOutputStream(outputStream);
      try {
        this.encoder = valueEncoder.create(outputStream);
      } catch (IOException e) {
        e.printStackTrace();
        throw new RuntimeException(e);
      }
    }

    @Override
    public void encode(final List<T> value) throws IOException {
      outputStream.writeInt(value.size());
      for (final T val : value) {
        encoder.encode(val);
      }
    }
  }
}
