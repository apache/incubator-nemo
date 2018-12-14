package org.apache.nemo.common;

public final class GBKLambdaEvent {

  public enum Type {
    START,
    DATA,
    END
  }

  public final Object data;
  public final Type type;

  public GBKLambdaEvent(final Type type,
                        final Object data) {
    this.type = type;
    this.data = data;
  }
}
