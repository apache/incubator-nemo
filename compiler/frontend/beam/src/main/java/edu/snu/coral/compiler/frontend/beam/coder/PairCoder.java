/*
 * Copyright (C) 2017 Seoul National University
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
package edu.snu.coral.compiler.frontend.beam.coder;

import edu.snu.coral.common.Pair;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.CoderException;
import org.apache.beam.sdk.coders.StructuredCoder;
import org.apache.beam.sdk.util.common.ElementByteSizeObserver;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Arrays;
import java.util.List;

/**
 * BEAM Coder for {@link edu.snu.coral.common.Pair}. Reference: KvCoder in BEAM.
 * @param <A> type for the left coder.
 * @param <B> type for the right coder.
 */
public final class PairCoder<A, B> extends StructuredCoder<Pair<A, B>> {
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

  //=====================================================================================================

  @Override
  public void encode(final Pair<A, B> pair, final OutputStream outStream) throws IOException {
    if (pair == null) {
      throw new CoderException("cannot encode a null KV");
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

  @Override
  public List<? extends Coder<?>> getCoderArguments() {
    return Arrays.asList(leftCoder, rightCoder);
  }

  @Override
  public void verifyDeterministic() throws NonDeterministicException {
    verifyDeterministic(this, "Key coder must be deterministic", getLeftCoder());
    verifyDeterministic(this, "Value coder must be deterministic", getRightCoder());
  }

  @Override
  public boolean consistentWithEquals() {
    return leftCoder.consistentWithEquals() && rightCoder.consistentWithEquals();
  }

  @Override
  public Object structuralValue(final Pair<A, B> pair) {
    if (consistentWithEquals()) {
      return pair;
    } else {
      return Pair.of(getLeftCoder().structuralValue(pair.left()), getRightCoder().structuralValue(pair.right()));
    }
  }

  /**
   * Returns whether both leftCoder and rightCoder are considered not expensive.
   */
  @Override
  public boolean isRegisterByteSizeObserverCheap(final Pair<A, B> pair) {
    return leftCoder.isRegisterByteSizeObserverCheap(pair.left())
        && rightCoder.isRegisterByteSizeObserverCheap(pair.right());
  }

  /**
   * Notifies ElementByteSizeObserver about the byte size of the
   * encoded value using this coder.
   */
  @Override
  public void registerByteSizeObserver(final Pair<A, B> pair,
                                       final ElementByteSizeObserver observer) throws Exception {
    if (pair == null) {
      throw new CoderException("cannot encode a null Pair");
    }
    leftCoder.registerByteSizeObserver(pair.left(), observer);
    rightCoder.registerByteSizeObserver(pair.right(), observer);
  }
}
