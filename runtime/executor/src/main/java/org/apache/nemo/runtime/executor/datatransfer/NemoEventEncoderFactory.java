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
package org.apache.nemo.runtime.executor.datatransfer;

import org.apache.commons.lang.SerializationUtils;
import org.apache.nemo.common.coder.EncoderFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.OutputStream;
import java.io.Serializable;

/**
 * A factory for NemoEventEncoder.
 */
public final class NemoEventEncoderFactory implements EncoderFactory {
  private static final Logger LOG = LoggerFactory.getLogger(NemoEventEncoderFactory.class.getName());

  private final EncoderFactory valueEncoderFactory;

  public NemoEventEncoderFactory(final EncoderFactory valueEncoderFactory) {
    this.valueEncoderFactory = valueEncoderFactory;
  }

  @Override
  public Encoder create(final OutputStream outputStream) throws IOException {
    return new NemoEventEncoder(valueEncoderFactory.create(outputStream), outputStream);
  }

  @Override
  public String toString() {
    return "NemoEventEncoderFactory{"
      + "valueEncoderFactory=" + valueEncoderFactory
      + '}';
  }

  /**
   * This encodes normal data and WatermarkWithIndex.
   *
   * @param <T> type to encode.
   */
  private final class NemoEventEncoder<T> implements EncoderFactory.Encoder<T> {
    private final EncoderFactory.Encoder<T> valueEncoder;
    private final OutputStream outputStream;

    NemoEventEncoder(final EncoderFactory.Encoder<T> valueEncoder,
                     final OutputStream outputStream) {
      this.valueEncoder = valueEncoder;
      this.outputStream = outputStream;
    }

    @Override
    public void encode(final T element) throws IOException {
      if (element instanceof WatermarkWithIndex) {
        outputStream.write(0x01); // this is watermark
        outputStream.write(SerializationUtils.serialize((Serializable) element));
      } else {
        outputStream.write(0x00); // this is a data element
        valueEncoder.encode(element);
      }
    }
  }
}
