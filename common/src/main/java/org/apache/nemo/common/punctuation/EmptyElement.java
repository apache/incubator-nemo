package org.apache.nemo.common.punctuation;

public final class EmptyElement {
  private static final EmptyElement INSTANCE = new EmptyElement();

  public static EmptyElement getInstance() {
    return INSTANCE;
  }
}
