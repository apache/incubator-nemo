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

import org.apache.nemo.common.Pair;

import java.io.IOException;
import java.io.InputStream;
import java.io.Serializable;

/**
 * An DecoderFactory for {@link Pair}. Reference: KvCoder in BEAM.
 *
 * @param <A> type for the left coder.
 * @param <B> type for the right coder.
 */
public final class PairDecoderFactory<A extends Serializable, B extends Serializable>
  implements DecoderFactory<Pair<A, B>> {
  private final DecoderFactory<A> leftDecoderFactory;
  private final DecoderFactory<B> rightDecoderFactory;

  /**
   * Private constructor of PairDecoderFactory class.
   *
   * @param leftDecoderFactory  coder for right element.
   * @param rightDecoderFactory coder for right element.
   */
  private PairDecoderFactory(final DecoderFactory<A> leftDecoderFactory,
                             final DecoderFactory<B> rightDecoderFactory) {
    this.leftDecoderFactory = leftDecoderFactory;
    this.rightDecoderFactory = rightDecoderFactory;
  }

  /**
   * static initializer of the class.
   *
   * @param leftDecoderFactory  left coder.
   * @param rightDecoderFactory right coder.
   * @param <A>                 type of the left element.
   * @param <B>                 type of the right element.
   * @return the new PairDecoderFactory.
   */
  public static <A extends Serializable, B extends Serializable> PairDecoderFactory<A, B>
  of(final DecoderFactory<A> leftDecoderFactory, final DecoderFactory<B> rightDecoderFactory) {
    return new PairDecoderFactory<>(leftDecoderFactory, rightDecoderFactory);
  }

  @Override
  public Decoder<Pair<A, B>> create(final InputStream inputStream) throws IOException {
    return new PairDecoder<>(inputStream, leftDecoderFactory, rightDecoderFactory);
  }

  @Override
  public String toString() {
    final StringBuilder sb = new StringBuilder();
    sb.append("Pair(");
    sb.append(leftDecoderFactory.toString());
    sb.append(", ");
    sb.append(rightDecoderFactory.toString());
    sb.append(")");
    return sb.toString();
  }

  /**
   * PairDecoder.
   *
   * @param <T1> type for the left coder.
   * @param <T2> type for the right coder.
   */
  private final class PairDecoder<T1 extends Serializable, T2 extends Serializable> implements Decoder<Pair<T1, T2>> {

    private final Decoder<T1> leftDecoder;
    private final Decoder<T2> rightDecoder;

    /**
     * Constructor.
     *
     * @param inputStream         the input stream to decode.
     * @param leftDecoderFactory  the actual decoder to use for left elements.
     * @param rightDecoderFactory the actual decoder to use for right elements.
     * @throws IOException if fail to instantiate coders.
     */
    private PairDecoder(final InputStream inputStream,
                        final DecoderFactory<T1> leftDecoderFactory,
                        final DecoderFactory<T2> rightDecoderFactory) throws IOException {
      this.leftDecoder = leftDecoderFactory.create(inputStream);
      this.rightDecoder = rightDecoderFactory.create(inputStream);
    }

    @Override
    public Pair<T1, T2> decode() throws IOException {
      final T1 key = leftDecoder.decode();
      final T2 value = rightDecoder.decode();
      return Pair.of(key, value);
    }
  }
}
