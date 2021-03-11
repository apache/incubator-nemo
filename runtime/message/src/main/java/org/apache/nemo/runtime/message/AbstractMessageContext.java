package org.apache.nemo.runtime.message;

public abstract class AbstractMessageContext implements MessageContext {

  protected final long requestId;

  public AbstractMessageContext(final long requestId) {
    this.requestId = requestId;
  }

  @Override
  public long getRequestId() {
    return requestId;
  }
}
