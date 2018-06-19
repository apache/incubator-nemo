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

import edu.snu.nemo.common.Pair;

import java.io.IOException;
import java.io.InputStream;

/**
 * An DecoderFactory for {@link Pair}. Reference: KvCoder in BEAM.
 * @param <A> type for the left coder.
 * @param <B> type for the right coder.
 */
public final class PairDecoderFactory<A, B> implements DecoderFactory<Pair<A, B>> {
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
   * @param <A>          type of the left element.
   * @param <B>          type of the right element.
   * @return the new PairDecoderFactory.
   */
  public static <A, B> PairDecoderFactory<A, B> of(final DecoderFactory<A> leftDecoderFactory,
                                                   final DecoderFactory<B> rightDecoderFactory) {
    return new PairDecoderFactory<>(leftDecoderFactory, rightDecoderFactory);
  }

  @Override
  public Decoder<Pair<A, B>> create(final InputStream inputStream) throws IOException {
    return new PairDecoder<>(inputStream, leftDecoderFactory, rightDecoderFactory);
  }

  /**
   * PairDecoder.
   * @param <T1> type for the left coder.
   * @param <T2> type for the right coder.
   */
  private final class PairDecoder<T1, T2> implements Decoder<Pair<T1, T2>> {

    private final Decoder<T1> leftDecoder;
    private final Decoder<T2> rightDecoder;

    /**
     * Constructor.
     *
     * @param inputStream  the input stream to decode.
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
