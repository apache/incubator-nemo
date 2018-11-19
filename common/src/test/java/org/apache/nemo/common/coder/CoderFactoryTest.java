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

import org.junit.Assert;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;

/**
 * Tests coder factories.
 */
public class CoderFactoryTest {

  @Test
  public void testBytesCoderFactories() throws Exception {
    final BytesEncoderFactory encoderFactory = BytesEncoderFactory.of();
    final BytesDecoderFactory decoderFactory = BytesDecoderFactory.of();

    // Test empty bytes.
    byte[] elementToTest = new byte[0];
    byte[] decodedBytes = encodeAndDecodeElement(encoderFactory, decoderFactory, elementToTest);
    Assert.assertArrayEquals(elementToTest, decodedBytes);

    // Test filled bytes.
    elementToTest = "Hello NEMO!".getBytes();
    decodedBytes = encodeAndDecodeElement(encoderFactory, decoderFactory, elementToTest);
    Assert.assertArrayEquals(elementToTest, decodedBytes);
  }

  /**
   * Encode and decode an element through the given factories and return the result elements.
   *
   * @param encoderFactory the encoder factory to test.
   * @param decoderFactory the decoder factory to test.
   * @param element        the element to test.
   * @param <T>            the type of the element.
   * @return the decoded element.
   */
  private <T> T encodeAndDecodeElement(final EncoderFactory<T> encoderFactory,
                                                     final DecoderFactory<T> decoderFactory,
                                                     final T element) throws Exception {
    final byte[] encodedElement;
    try (final ByteArrayOutputStream out = new ByteArrayOutputStream()) {
      final EncoderFactory.Encoder<T> encoder = encoderFactory.create(out);
      encoder.encode(element);
      encodedElement = out.toByteArray();
    }

    final T decodedElement;
    try (final ByteArrayInputStream in = new ByteArrayInputStream(encodedElement)) {
      final DecoderFactory.Decoder<T> decoder = decoderFactory.create(in);
      decodedElement = decoder.decode();
    }

    return decodedElement;
  }
}
