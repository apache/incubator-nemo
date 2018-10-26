package org.apache.nemo.common.eventhandler;

/**
 * Class for handling events sent from Runtime.
 * @param <T> type of the runtime event to handle.
 */
public interface RuntimeEventHandler<T extends RuntimeEvent> extends CommonEventHandler<T> {
}
