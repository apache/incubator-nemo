package org.apache.nemo.runtime.master.servlet;

import org.apache.nemo.runtime.master.MetricStore;
import org.apache.nemo.runtime.common.metric.JobMetric;

import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;

/**
 * Servlet which handles {@link JobMetric} metric request.
 */
public final class JobMetricServlet extends HttpServlet {

  @Override
  protected void doGet(final HttpServletRequest request, final HttpServletResponse response)
          throws IOException {
    final MetricStore metricStore = MetricStore.getStore();
    response.setContentType("application/json");
    response.setStatus(HttpServletResponse.SC_OK);
    response.getWriter().println(metricStore.dumpMetricToJson(JobMetric.class));
  }
}
