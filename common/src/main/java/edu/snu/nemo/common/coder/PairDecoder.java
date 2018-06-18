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
 * An Decoder for {@link Pair}. Reference: KvCoder in BEAM.
 * @param <A> type for the left coder.
 * @param <B> type for the right coder.
 */
public final class PairDecoder<A, B> implements Decoder<Pair<A, B>> {
  private final Decoder<A> leftDecoder;
  private final Decoder<B> rightDecoder;

  /**
   * Private constructor of PairDecoder class.
   *
   * @param leftDecoder  coder for right element.
   * @param rightDecoder coder for right element.
   */
  private PairDecoder(final Decoder<A> leftDecoder,
                      final Decoder<B> rightDecoder) {
    this.leftDecoder = leftDecoder;
    this.rightDecoder = rightDecoder;
  }

  /**
   * static initializer of the class.
   *
   * @param leftDecoder  left coder.
   * @param rightDecoder right coder.
   * @param <A>          type of the left element.
   * @param <B>          type of the right element.
   * @return the new PairDecoder.
   */
  public static <A, B> PairDecoder<A, B> of(final Decoder<A> leftDecoder,
                                            final Decoder<B> rightDecoder) {
    return new PairDecoder<>(leftDecoder, rightDecoder);
  }

  @Override
  public DecoderInstance<Pair<A, B>> getDecoderInstance(final InputStream inputStream) throws IOException {
    return new PairDecoderInstance<>(inputStream, leftDecoder, rightDecoder);
  }

  /**
   * PairDecoderInstance.
   * @param <T1> type for the left coder.
   * @param <T2> type for the right coder.
   */
  private final class PairDecoderInstance<T1, T2> implements DecoderInstance<Pair<T1, T2>> {

    private final DecoderInstance<T1> leftDecoderInstance;
    private final DecoderInstance<T2> rightDecoderInstance;

    /**
     * Constructor.
     *
     * @param inputStream  the input stream to decode.
     * @param leftDecoder  the actual decoder to use for left elements.
     * @param rightDecoder the actual decoder to use for right elements.
     * @throws IOException if fail to instantiate coders.
     */
    private PairDecoderInstance(final InputStream inputStream,
                                final Decoder<T1> leftDecoder,
                                final Decoder<T2> rightDecoder) throws IOException {
      this.leftDecoderInstance = leftDecoder.getDecoderInstance(inputStream);
      this.rightDecoderInstance = rightDecoder.getDecoderInstance(inputStream);
    }

    @Override
    public Pair<T1, T2> decode() throws IOException {
      final T1 key = leftDecoderInstance.decode();
      final T2 value = rightDecoderInstance.decode();
      return Pair.of(key, value);
    }
  }
}
