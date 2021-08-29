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

package org.apache.nemo.runtime.master.scheduler;

import org.apache.nemo.conf.JobConf;
import org.apache.nemo.runtime.common.metric.Metric;
import org.apache.nemo.runtime.common.plan.PhysicalPlan;
import org.apache.nemo.runtime.common.plan.Task;
import org.apache.nemo.runtime.master.metric.MetricStore;
import org.apache.reef.tang.annotations.Parameter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import java.util.ArrayList;
import java.util.List;

/**
 * Simulator for stream processing.
 */
public class StreamingPlanSimulator extends PlanSimulator {
  private static final Logger LOG = LoggerFactory.getLogger(StreamingPlanSimulator.class.getName());

  /**
   * The metric store for the simulation.
   */
  private MetricStore metricStore;

  /**
   * The below variables depend on the submitted plan to execute.
   */
  @Inject
  public StreamingPlanSimulator(final ScheduleSimulator scheduler,
                                @Parameter(JobConf.ExecutorJSONContents.class) final String resourceSpecificationString,
                                @Parameter(JobConf.NodeSpecJsonContents.class) final String nodeSpecificationString)
    throws Exception {
    super(scheduler, resourceSpecificationString, nodeSpecificationString);
    LOG.info("Start StreamPlanSimulator");
    this.metricStore = MetricStore.newInstance();
  }

  /**
   * Reset the instance to its initial state.
   */
  public void reset() throws Exception {
    super.reset();
    this.terminate();
    metricStore = MetricStore.newInstance();
  }

  /**
   * Simulate Plan.
   *
   * @param plan               to execute
   * @param maxScheduleAttempt the max number of times this plan/sub-part of the plan should be attempted.
   */
  public void simulate(final PhysicalPlan plan,
                      final int maxScheduleAttempt) throws Exception {
    // distribute tasks to executors
    getScheduler().schedulePlan(plan, maxScheduleAttempt);

    // prepare tasks to simulate.
    getContainerManager().prepare();

    // distribute resources to tasks.
    getContainerManager().getNodeSimulators().forEach(NodeSimulator::distributeResource);
    List<Task> tasks = getContainerManager().getTasks();

    // get topological sorted tasks.
    List<Task> sortedTasks = new ArrayList<>();
    plan.getStageDAG().getTopologicalSort().forEach(stage -> {
      for (Task task : tasks) {
        if (task.getStageId().equals(stage.getId())) {
          sortedTasks.add(task);
        }
      }
    });

    long startTimestamp = System.currentTimeMillis();

    // TODO XXX: Interval and maximum timestamp should be researched.
    long maximumTimeStamp = 200000;
    long timestamp = 0;
    long duration = 2000;

    LOG.info("Start simulation");

    // iterate until timestamp reached maximumTimeStamp;
    while (maximumTimeStamp > timestamp) {
      for (Task task : sortedTasks) {
        getContainerManager().simulate(task, duration);
      }
      timestamp += duration;
    }
    LOG.info("Time to simulate: " + (System.currentTimeMillis() - startTimestamp));
  }

  /**
   * Record metrics in the metricStore.
   */
  public void onMetricMessageReceived(final String metricType,
                                      final String metricId,
                                      final String metricField,
                                      final byte[] metricValue) {
      final Class<Metric> metricClass = metricStore.getMetricClassByName(metricType);
      metricStore.getOrCreateMetric(metricClass, metricId).processMetricMessage(metricField, metricValue);
  }

  /**
   * get throughput.
   */
  // TODO XXX: Implement this method
  public float getThroughput() {
    return 0;
  }

  /**
   * get latency.
   */
  // TODO XXX: Implement this method
  public float getLatency() {
    return 0;
  }
}
