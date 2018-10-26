package org.apache.nemo.common.eventhandler;

import org.apache.reef.wake.EventHandler;

/**
 * Class for handling common events.
 * @param <T> type of the event to handle.
 */
public interface CommonEventHandler<T> extends EventHandler<T> {
  /**
   * fetches the class of the event.
   * @return the class of the event.
   */
  Class<T> getEventClass();
}
