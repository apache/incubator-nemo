package org.apache.nemo.common;

public final class Triple<A, B, C> {
  public A first;
  public B second;
  public C third;

  public Triple(final A a, final B b, final C c) {
    this.first = a;
    this.second = b;
    this.third = c;
  }
}
