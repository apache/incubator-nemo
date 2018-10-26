package org.apache.nemo.runtime.master;

import org.eclipse.jetty.websocket.api.Session;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * MetricBroadcaster broadcast metric changes to the currently active WebSocket sessions.
 */
public final class MetricBroadcaster {
  private static final Logger LOG = LoggerFactory.getLogger(MetricBroadcaster.class.getName());
  private final Set<Session> sessions = ConcurrentHashMap.newKeySet();
  /**
   * Private constructor.
   */
  private MetricBroadcaster() { }

  /**
   * Getter for the singleton object.
   * @return MetricBroadcaster object.
   */
  public static MetricBroadcaster getInstance() {
    return InstanceHolder.INSTANCE;
  }

  /**
   * Lazy class object holder for MetricBroadcaster class.
   */
  private static class InstanceHolder {
    private static final MetricBroadcaster INSTANCE = new MetricBroadcaster();
  }

  /**
   * Add a session to the session list.
   * @param session a WebSocket session.
   */
  public synchronized void addSession(final Session session) {
    try {
      session.getRemote().sendString(MetricStore.getStore().dumpAllMetricToJson());
    } catch (final IOException e) {
      LOG.warn("Failed to send initial metric to newly connected session.");
    }
    sessions.add(session);
  }

  /**
   * Remove a session from the session list.
   * @param session a WebSocket session.
   */
  public synchronized void removeSession(final Session session) {
    sessions.remove(session);
  }

  /**
   * Send text frame to each WebSocket session.
   * @param text text to send.
   */
  public void broadcast(final String text) {
    for (final Session session : sessions) {
      try {
        session.getRemote().sendString(text);
      } catch (final IOException e) {
        LOG.warn("Failed to send string to remote session {}.", session.getRemoteAddress().toString());
      }
    }
  }

  /**
   * Send binary frame to each WebSocket session.
   * @param bytes byte array to send.
   */
  public void broadcast(final byte[] bytes) {
    for (final Session session : sessions) {
      try {
        session.getRemote().sendBytes(ByteBuffer.wrap(bytes));
      } catch (final IOException e) {
        LOG.warn("Failed to send binary to remote session {}.", session.getRemoteAddress().toString());
      }
    }
  }

}
