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

import edu.snu.nemo.common.ContextImpl;
import edu.snu.nemo.common.Pair;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.junit.Assert;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.OutputStream;
import java.util.HashMap;
import java.util.Map;

/**
 * Tests {@link ContextImpl}.
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
  
  /*
  @Test
  public void testMapCoderFactories() throws Exception {
    final MapEncoderFactory encoderFactory = MapEncoderFactory.of(IntEncoderFactory.of(), IntEncoderFactory.of());
    final MapDecoderFactory decoderFactory = MapDecoderFactory.of(IntDecoderFactory.of(), IntDecoderFactory.of());
  
    // Test filled bytes.
    Map<Integer, Integer> map = new HashMap<>();
    map.put(1, 10);
    map.put(2, 20);
    map.put(3, 30);
    Map<Integer, Long> decodedBytes = encodeAndDecodeElement2(encoderFactory, decoderFactory, map);
    Assert.assertEquals(map, decodedBytes);
  }
  
  private <K, V> Map<K, V> encodeAndDecodeElement2(final MapEncoderFactory<K, V> encoderFactory,
                                       final MapDecoderFactory<K, V> decoderFactory,
                                       Map<K, V> map) throws Exception {
    final byte[] encodedElement;
    try (final ByteArrayOutputStream out = new ByteArrayOutputStream()) {
      final EncoderFactory.Encoder<Map<K, V>> encoder = encoderFactory.create(out);
      encoder.encode(map);
      encodedElement = out.toByteArray();
    }

    final Map<K, V> decodedElement;
    try (final ByteArrayInputStream in = new ByteArrayInputStream(encodedElement)) {
      final DecoderFactory.Decoder<Map<K, V>> decoder = decoderFactory.create(in);
      decodedElement = decoder.decode();
    }
    
    return decodedElement;
  }
  */
  
  @Test
  public void testPairCoderFactories() throws Exception {
    final PairEncoderFactory encoderFactory = PairEncoderFactory.of(IntEncoderFactory.of(), LongEncoderFactory.of());
    final PairDecoderFactory decoderFactory = PairDecoderFactory.of(IntDecoderFactory.of(), LongDecoderFactory.of());
    
    // Test filled bytes.
    Pair<Integer, Long> pair = Pair.of(1, 10L);
    Pair<Integer, Long> decodedBytes = encodeAndDecodeElement3(encoderFactory, decoderFactory, pair);
    Assert.assertEquals(pair, decodedBytes);
  }
  
  private <K, V> Pair<K, V> encodeAndDecodeElement3(final PairEncoderFactory<K, V> encoderFactory,
                                                    final PairDecoderFactory<K, V> decoderFactory,
                                                    Pair<K, V> pair) throws Exception {
    final byte[] encodedElement;
    try (final ByteArrayOutputStream out = new ByteArrayOutputStream()) {
      final EncoderFactory.Encoder<Pair<K, V>> encoder = encoderFactory.create(out);
      encoder.encode(pair);
      encodedElement = out.toByteArray();
    }
    
    final Pair<K, V> decodedElement;
    try (final ByteArrayInputStream in = new ByteArrayInputStream(encodedElement)) {
      final DecoderFactory.Decoder<Pair<K, V>> decoder = decoderFactory.create(in);
      decodedElement = decoder.decode();
    }
    
    return decodedElement;
  }
}
