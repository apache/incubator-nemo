/*
 * Copyright (C) 2018 Seoul National University
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package edu.snu.nemo.driver;

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
