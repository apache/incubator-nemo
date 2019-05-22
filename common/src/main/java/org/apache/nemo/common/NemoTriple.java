package org.apache.nemo.common;

import java.io.Serializable;
import java.util.Objects;

public final class NemoTriple<A, B, C> implements Serializable {

  public final A first;
  public final B second;
  public final C third;

  public NemoTriple(final A first,
                    final B second,
                    final C third) {
    this.first = first;
    this.second = second;
    this.third = third;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    NemoTriple<?, ?, ?> that = (NemoTriple<?, ?, ?>) o;
    return Objects.equals(first, that.first) &&
      Objects.equals(second, that.second) &&
      Objects.equals(third, that.third);
  }

  @Override
  public int hashCode() {

    return Objects.hash(first, second, third);
  }
}
