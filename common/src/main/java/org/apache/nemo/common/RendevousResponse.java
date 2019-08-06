package org.apache.nemo.common;

public final class RendevousResponse {

  public final String dst;
  public final String address;

  public RendevousResponse(final String dst,
                           final String address) {
    this.dst = dst;
    this.address = address;
  }
}
