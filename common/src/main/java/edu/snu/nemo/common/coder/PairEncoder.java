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
 * An Encoder for {@link Pair}. Reference: KvCoder in BEAM.
 * @param <A> type for the left coder.
 * @param <B> type for the right coder.
 */
public final class PairEncoder<A, B> implements Encoder<Pair<A, B>> {
  private final Encoder<A> leftEncoder;
  private final Encoder<B> rightEncoder;

  /**
   * Private constructor of PairEncoder class.
   *
   * @param leftEncoder  coder for right element.
   * @param rightEncoder coder for right element.
   */
  private PairEncoder(final Encoder<A> leftEncoder,
                      final Encoder<B> rightEncoder) {
    this.leftEncoder = leftEncoder;
    this.rightEncoder = rightEncoder;
  }

  /**
   * static initializer of the class.
   *
   * @param leftEncoder  left coder.
   * @param rightEncoder right coder.
   * @param <A>          type of the left element.
   * @param <B>          type of the right element.
   * @return the new PairEncoder.
   */
  public static <A, B> PairEncoder<A, B> of(final Encoder<A> leftEncoder,
                                            final Encoder<B> rightEncoder) {
    return new PairEncoder<>(leftEncoder, rightEncoder);
  }

  @Override
  public EncoderInstance<Pair<A, B>> getEncoderInstance(final OutputStream outputStream) throws IOException {
    return new PairEncoderInstance<>(outputStream, leftEncoder, rightEncoder);
  }

  /**
   * PairEncoderInstance.
   * @param <T1> type for the left coder.
   * @param <T2> type for the right coder.
   */
  private final class PairEncoderInstance<T1, T2> implements EncoderInstance<Pair<T1, T2>> {

    private final EncoderInstance<T1> leftEncoderInstance;
    private final EncoderInstance<T2> rightEncoderInstance;

    /**
     * Constructor.
     *
     * @param outputStream the output stream to store the encoded bytes.
     * @param leftEncoder  the actual encoder to use for left elements.
     * @param rightEncoder the actual encoder to use for right elements.
     * @throws IOException if fail to instantiate coders.
     */
    private PairEncoderInstance(final OutputStream outputStream,
                                final Encoder<T1> leftEncoder,
                                final Encoder<T2> rightEncoder) throws IOException {
      this.leftEncoderInstance = leftEncoder.getEncoderInstance(outputStream);
      this.rightEncoderInstance = rightEncoder.getEncoderInstance(outputStream);
    }

    @Override
    public void encode(final Pair<T1, T2> pair) throws IOException {
      if (pair == null) {
        throw new IOException("cannot encode a null pair");
      }
      leftEncoderInstance.encode(pair.left());
      rightEncoderInstance.encode(pair.right());
    }
  }
}
