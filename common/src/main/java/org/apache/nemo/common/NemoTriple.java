package org.apache.nemo.common;

public final class NemoTriple<A, B, C> {

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
}
