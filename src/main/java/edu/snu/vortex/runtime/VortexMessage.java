package edu.snu.vortex.runtime;

import java.io.Serializable;

public final class VortexMessage implements Serializable {

  private Type type;

  public VortexMessage(final Type type) {
    this.type = type;
  }

  public Type getType() {
    return type;
  }

  public enum Type {
  }
}
