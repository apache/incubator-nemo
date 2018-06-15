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
package edu.snu.nemo.common;

import java.io.Serializable;
import java.util.Objects;

/**
 * Pair class.
 * @param <A> type of the left element.
 * @param <B> type of the right element.
 */
public final class Pair<A, B> implements Serializable {
  private final A left;
  private final B right;

  /**
   * Private constructor for Pair class.
   * @param left left element.
   * @param right right element.
   */
  private Pair(final A left, final B right) {
    this.left = left;
    this.right = right;
  }

  /**
   * @return left element.
   */
  public A left() {
    return left;
  }
  /**
   * @return right element
   */
  public B right() {
    return right;
  }

  @Override
  public String toString() {
    return "Pair[" + this.left + "," + this.right + "]";
  }

  @Override
  public boolean equals(final Object obj) {
    return obj instanceof Pair
        && Objects.equals(this.left, ((Pair) obj).left)
        && Objects.equals(this.right, ((Pair) obj).right);
  }

  @Override
  public int hashCode() {
    return this.left == null
        ? (this.right == null ? 0 : this.right.hashCode() + 1)
        : (this.right == null ? this.left.hashCode() + 2 : this.left.hashCode() * 17 + this.right.hashCode());
  }

  /**
   * Static initializer of the Pair class.
   * @param left left element.
   * @param right right element.
   * @param <A> Type of the left element.
   * @param <B> Type of the right element.
   * @return the newly created Pair.
   */
  public static <A, B> Pair<A, B> of(final A left, final B right) {
    return new Pair<>(left, right);
  }
}
