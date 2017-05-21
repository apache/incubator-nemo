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
package edu.snu.vortex.compiler.frontend.beam.coder;

import edu.snu.vortex.utils.Pair;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.CoderException;
import org.apache.beam.sdk.coders.StandardCoder;
import org.apache.beam.sdk.repackaged.com.google.common.base.Preconditions;
import org.apache.beam.sdk.util.CloudObject;
import org.apache.beam.sdk.util.PropertyNames;
import org.apache.beam.sdk.util.common.ElementByteSizeObserver;
import org.codehaus.jackson.annotate.JsonCreator;
import org.codehaus.jackson.annotate.JsonProperty;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Arrays;
import java.util.List;

import static org.apache.beam.sdk.util.Structs.addBoolean;

/**
 * BEAM Coder for {@link edu.snu.vortex.utils.Pair}.
 * @param <A> type for the left coder.
 * @param <B> type for the right coder.
 */
public final class PairCoder<A, B> extends StandardCoder<Pair<A, B>> {
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
   * Same static initializer in JSON.
   * @param components compoenents.
   * @return the newly created PairCoder.
   */
  @JsonCreator
  public static PairCoder<?, ?> of(@JsonProperty(PropertyNames.COMPONENT_ENCODINGS) final List<Coder<?>> components) {
    Preconditions.checkArgument(components.size() == 2, "Expecting 2 components, got " + components.size());
    return of(components.get(0), components.get(1));
  }

  /**
   * @param exampleValue example input value.
   * @param <A> type of left element of example value.
   * @param <B> type of right element of example value.
   * @return instance components for the example value.
   */
  public static <A, B> List<Object> getInstanceComponents(final Pair<A, B> exampleValue) {
    return Arrays.asList(exampleValue.left(), exampleValue.right());
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
  public void encode(final Pair<A, B> pair, final OutputStream outStream, final Context context) throws IOException {
    if (pair == null) {
      throw new CoderException("cannot encode a null KV");
    }
    final Context nestedContext = context.nested();
    leftCoder.encode(pair.left(), outStream, nestedContext);
    rightCoder.encode(pair.right(), outStream, nestedContext);
  }

  @Override
  public Pair<A, B> decode(final InputStream inStream, final Context context) throws IOException {
    final Context nestedContext = context.nested();
    final A key = leftCoder.decode(inStream, nestedContext);
    final B value = rightCoder.decode(inStream, nestedContext);
    return Pair.of(key, value);
  }

  @Override
  public List<? extends Coder<?>> getCoderArguments() {
    return Arrays.asList(leftCoder, rightCoder);
  }

  @Override
  public void verifyDeterministic() throws NonDeterministicException {
    verifyDeterministic("Key coder must be deterministic", getLeftCoder());
    verifyDeterministic("Value coder must be deterministic", getRightCoder());
  }

  @Override
  public boolean consistentWithEquals() {
    return leftCoder.consistentWithEquals() && rightCoder.consistentWithEquals();
  }

  @Override
  public Object structuralValue(final Pair<A, B> pair) throws Exception {
    if (consistentWithEquals()) {
      return pair;
    } else {
      return Pair.of(getLeftCoder().structuralValue(pair.left()), getRightCoder().structuralValue(pair.right()));
    }
  }

  @Override
  protected CloudObject initializeCloudObject() {
    final CloudObject result = CloudObject.forClass(getClass());
    addBoolean(result, PropertyNames.IS_PAIR_LIKE, true);
    return result;
  }

  /**
   * Returns whether both leftCoder and rightCoder are considered not expensive.
   */
  @Override
  public boolean isRegisterByteSizeObserverCheap(final Pair<A, B> pair, final Context context) {
    return leftCoder.isRegisterByteSizeObserverCheap(pair.left(), context.nested())
        && rightCoder.isRegisterByteSizeObserverCheap(pair.right(), context.nested());
  }

  /**
   * Notifies ElementByteSizeObserver about the byte size of the
   * encoded value using this coder.
   */
  @Override
  public void registerByteSizeObserver(final Pair<A, B> pair,
                                       final ElementByteSizeObserver observer,
                                       final Context context) throws Exception {
    if (pair == null) {
      throw new CoderException("cannot encode a null Pair");
    }
    leftCoder.registerByteSizeObserver(pair.left(), observer, context.nested());
    rightCoder.registerByteSizeObserver(pair.right(), observer, context.nested());
  }
}
