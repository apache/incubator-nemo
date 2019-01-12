/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.nemo.runtime.master.servlet;

import org.apache.nemo.runtime.master.MetricBroadcaster;
import org.eclipse.jetty.websocket.api.Session;
import org.eclipse.jetty.websocket.api.StatusCode;
import org.eclipse.jetty.websocket.api.WebSocketAdapter;

import java.io.IOException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Adapter for WebSocket metric request and response.
 */
public class WebSocketMetricAdapter extends WebSocketAdapter {
  private static final Logger LOG = LoggerFactory.getLogger(WebSocketMetricAdapter.class.getName());
  private Session session;

  @Override
  public final void onWebSocketConnect(final Session sess) {
    this.session = sess;
    MetricBroadcaster.getInstance().addSession(this.session);
  }

  @Override
  public final void onWebSocketClose(final int statusCode, final String reason) {
    if (statusCode != StatusCode.NORMAL) {
      LOG.warn("WebSocket session closed abnormally: {} - {}.", statusCode, reason);
    }
    MetricBroadcaster.getInstance().removeSession(session);
  }

  @Override
  public final void onWebSocketError(final Throwable throwable) {
    MetricBroadcaster.getInstance().removeSession(session);
  }

  @Override
  public final void onWebSocketText(final String text) {
    try {
      session.getRemote().sendString(text);
    } catch (final IOException e) {
      throw new RuntimeException(e);
    }
  }
}
