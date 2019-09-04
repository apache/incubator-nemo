package org.apache.nemo.runtime.lambda;

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
