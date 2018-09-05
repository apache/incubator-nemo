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
import java.io.OutputStream;

/**
 * An EncoderFactory for {@link Pair}. Reference: KvCoder in BEAM.
 * @param <A> type for the left coder.
 * @param <B> type for the right coder.
 */
public final class PairEncoderFactory<A, B> implements EncoderFactory<Pair<A, B>> {
  private final EncoderFactory<A> leftEncoderFactory;
  private final EncoderFactory<B> rightEncoderFactory;

  /**
   * Private constructor of PairEncoderFactory class.
   *
   * @param leftEncoderFactory  coder for right element.
   * @param rightEncoderFactory coder for right element.
   */
  private PairEncoderFactory(final EncoderFactory<A> leftEncoderFactory,
                             final EncoderFactory<B> rightEncoderFactory) {
    this.leftEncoderFactory = leftEncoderFactory;
    this.rightEncoderFactory = rightEncoderFactory;
  }

  /**
   * static initializer of the class.
   *
   * @param leftEncoderFactory  left coder.
   * @param rightEncoderFactory right coder.
   * @param <A>          type of the left element.
   * @param <B>          type of the right element.
   * @return the new PairEncoderFactory.
   */
  public static <A, B> PairEncoderFactory<A, B> of(final EncoderFactory<A> leftEncoderFactory,
                                                   final EncoderFactory<B> rightEncoderFactory) {
    return new PairEncoderFactory<>(leftEncoderFactory, rightEncoderFactory);
  }

  @Override
  public Encoder<Pair<A, B>> create(final OutputStream outputStream) throws IOException {
    return new PairEncoder<>(outputStream, leftEncoderFactory, rightEncoderFactory);
  }

  /**
   * PairEncoder.
   * @param <T1> type for the left coder.
   * @param <T2> type for the right coder.
   */
  private final class PairEncoder<T1, T2> implements Encoder<Pair<T1, T2>> {

    private final Encoder<T1> leftEncoder;
    private final Encoder<T2> rightEncoder;

    /**
     * Constructor.
     *
     * @param outputStream the output stream to store the encoded bytes.
     * @param leftEncoderFactory  the actual encoder to use for left elements.
     * @param rightEncoderFactory the actual encoder to use for right elements.
     * @throws IOException if fail to instantiate coders.
     */
    private PairEncoder(final OutputStream outputStream,
                        final EncoderFactory<T1> leftEncoderFactory,
                        final EncoderFactory<T2> rightEncoderFactory) throws IOException {
      this.leftEncoder = leftEncoderFactory.create(outputStream);
      this.rightEncoder = rightEncoderFactory.create(outputStream);
    }

    @Override
    public void encode(final Pair<T1, T2> pair) throws IOException {
      if (pair == null) {
        throw new IOException("cannot encode a null pair");
      }
      leftEncoder.encode(pair.left());
      rightEncoder.encode(pair.right());
    }
  }
}
