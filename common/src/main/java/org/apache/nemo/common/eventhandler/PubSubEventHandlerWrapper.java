package org.apache.nemo.common.eventhandler;

import org.apache.reef.wake.impl.PubSubEventHandler;

import javax.inject.Inject;

/**
 * Wrapper class with a pub/sub event handler.
 */
public final class PubSubEventHandlerWrapper {
  private final PubSubEventHandler pubSubEventHandler;

  /**
   * Default constructor.
   */
  @Inject
  private PubSubEventHandlerWrapper() {
    this.pubSubEventHandler = new PubSubEventHandler();
  }

  /**
   * @return the pub/sub event handler.
   */
  public PubSubEventHandler getPubSubEventHandler() {
    return pubSubEventHandler;
  }
}
