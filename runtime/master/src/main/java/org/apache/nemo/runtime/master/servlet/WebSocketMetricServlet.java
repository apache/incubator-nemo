package org.apache.nemo.runtime.master.servlet;

import org.eclipse.jetty.websocket.servlet.WebSocketServlet;
import org.eclipse.jetty.websocket.servlet.WebSocketServletFactory;

/**
 * Servlet which handles WebSocket HTTP request.
 */
public class WebSocketMetricServlet extends WebSocketServlet {

  @Override
  public final void configure(final WebSocketServletFactory factory) {
    // registers WebSocket adapter
    factory.register(WebSocketMetricAdapter.class);
  }
}
