package org.apache.nemo.driver;

import org.apache.reef.driver.client.JobMessageObserver;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.logging.Handler;
import java.util.logging.LogRecord;
import java.util.logging.SimpleFormatter;

/**
 * A simple logging handler that forwards messages to REEF client.
 */
public final class RemoteClientMessageLoggingHandler extends Handler {

  private final JobMessageObserver client;
  private final SimpleFormatter formatter;
  private final AtomicBoolean closed;

  public RemoteClientMessageLoggingHandler(final JobMessageObserver client) {
    this.client = client;
    this.closed = new AtomicBoolean(false);
    this.formatter = new SimpleFormatter();
  }

  /**
   * TODO #XXX: buffer the log messages for performance.
   */
  @Override
  public void publish(final LogRecord record) {
    final StringBuilder sb = new StringBuilder();
    sb.append(formatter.format(record));

    if (!closed.get()) {
      client.sendMessageToClient(sb.toString().getBytes());
    }
  }

  @Override
  public void flush() {

  }

  @Override
  public void close() throws SecurityException {
    closed.set(true);
  }
}
