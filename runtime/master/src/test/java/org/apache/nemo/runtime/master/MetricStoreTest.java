package org.apache.nemo.runtime.master;

import com.fasterxml.jackson.core.TreeNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.nemo.runtime.common.metric.JobMetric;
import org.junit.Test;

import java.io.IOException;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

/**
 * Test for {@link MetricStore}
 */
public final class MetricStoreTest {
  @Test
  public void testJson() throws IOException {
    final MetricStore metricStore = MetricStore.getStore();

    metricStore.getOrCreateMetric(JobMetric.class, "testId");

    final String json = metricStore.dumpMetricToJson(JobMetric.class);

    final ObjectMapper objectMapper = new ObjectMapper();
    final TreeNode treeNode = objectMapper.readTree(json);

    final TreeNode jobMetricNode = treeNode.get("JobMetric");
    assertNotNull(jobMetricNode);

    final TreeNode metricNode = jobMetricNode.get("testId");
    assertNotNull(metricNode);

    final TreeNode fieldNode = metricNode.get("id");
    assertTrue(fieldNode.isValueNode());
  }
}
