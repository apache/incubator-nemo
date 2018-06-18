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
import java.io.OutputStream;

/**
 * A Coder for {@link edu.snu.nemo.common.Pair}. Reference: KvCoder in BEAM.
 * @param <A> type for the left coder.
 * @param <B> type for the right coder.
 */
public final class PairCoder<A, B> implements Coder<Pair<A, B>> {
  private final Coder<A> leftCoder;
  private final Coder<B> rightCoder;

  /**
   * Private constructor of PairCoder class.
   * @param leftCoder coder for right element.
   * @param rightCoder coder for right element.
   */
  private PairCoder(final Coder<A> leftCoder, final Coder<B> rightCoder) {
    this.leftCoder = leftCoder;
    this.rightCoder = rightCoder;
  }

  /**
   * static initializer of the class.
   * @param leftCoder left coder.
   * @param rightCoder right coder.
   * @param <A> type of the left element.
   * @param <B> type of the right element.
   * @return the new PairCoder.
   */
  public static <A, B> PairCoder<A, B> of(final Coder<A> leftCoder, final Coder<B> rightCoder) {
    return new PairCoder<>(leftCoder, rightCoder);
  }

  /**
   * @return the left coder.
   */
  Coder<A> getLeftCoder() {
    return leftCoder;
  }

  /**
   * @return the right coder.
   */
  Coder<B> getRightCoder() {
    return rightCoder;
  }

  @Override
  public void encode(final Pair<A, B> pair, final OutputStream outStream) throws IOException {
    if (pair == null) {
      throw new IOException("cannot encode a null pair");
    }
    leftCoder.encode(pair.left(), outStream);
    rightCoder.encode(pair.right(), outStream);
  }

  @Override
  public Pair<A, B> decode(final InputStream inStream) throws IOException {
    final A key = leftCoder.decode(inStream);
    final B value = rightCoder.decode(inStream);
    return Pair.of(key, value);
  }
}
