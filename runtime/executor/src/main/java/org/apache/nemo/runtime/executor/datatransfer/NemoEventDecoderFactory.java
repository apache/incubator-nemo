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
import org.apache.nemo.common.coder.DecoderFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;

/**
 * A factory for NemoEventDecoder.
 */
public final class NemoEventDecoderFactory implements DecoderFactory {
  private static final Logger LOG = LoggerFactory.getLogger(NemoEventDecoderFactory.class.getName());

  private final DecoderFactory valueDecoderFactory;

  public NemoEventDecoderFactory(final DecoderFactory valueDecoderFactory) {
    this.valueDecoderFactory = valueDecoderFactory;
  }

  public DecoderFactory getValueDecoderFactory() {
    return valueDecoderFactory;
  }

  @Override
  public Decoder create(final InputStream inputStream) throws IOException {
    return new NemoEventDecoder(valueDecoderFactory.create(inputStream), inputStream);
  }

  /**
   * This class decodes receive data into two types.
   * - normal data
   * - WatermarkWithIndex
   */
  private final class NemoEventDecoder implements DecoderFactory.Decoder {

    private final Decoder valueDecoder;
    private final InputStream inputStream;

    NemoEventDecoder(final Decoder valueDecoder,
                     final InputStream inputStream) {
      this.valueDecoder = valueDecoder;
      this.inputStream = inputStream;
    }

    @Override
    public Object decode() throws IOException {

      final byte isWatermark = (byte) inputStream.read();
      if (isWatermark == -1) {
        // end of the input stream
        throw new EOFException();
      }

      if (isWatermark == 0x00) {
        // this is not a watermark
        return valueDecoder.decode();
      } else if (isWatermark == 0x01) {
        // this is a watermark
        final WatermarkWithIndex watermarkWithIndex =
          (WatermarkWithIndex) SerializationUtils.deserialize(inputStream);
        return watermarkWithIndex;
      } else {
        throw new RuntimeException("Watermark decoding failure: " + isWatermark);
      }
    }

    @Override
    public String toString() {
      final StringBuilder stringBuilder = new StringBuilder("NemoDecoder{");
      stringBuilder.append(valueDecoder.toString());
      stringBuilder.append("}");
      return stringBuilder.toString();
    }
  }
}
