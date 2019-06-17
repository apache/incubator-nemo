package org.apache.nemo.runtime.executor.common;

public final class EmptyElement {
  private static final EmptyElement INSTANCE = new EmptyElement();

  public static EmptyElement getInstance() {
    return INSTANCE;
  }
}
