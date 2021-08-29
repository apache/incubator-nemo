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

import org.apache.commons.lang3.SerializationUtils;
import org.apache.nemo.common.Pair;
import org.apache.nemo.common.Util;
import org.apache.nemo.common.exception.*;
import org.apache.nemo.common.ir.executionproperty.ResourceSpecification;
import org.apache.nemo.runtime.common.comm.ControlMessage;
import org.apache.nemo.runtime.common.message.*;
import org.apache.nemo.runtime.common.plan.*;
import org.apache.nemo.runtime.master.resource.DefaultExecutorRepresenter;
import org.apache.nemo.runtime.master.resource.ExecutorRepresenter;
import org.apache.reef.driver.context.ActiveContext;
import org.apache.reef.driver.evaluator.EvaluatorDescriptor;
import org.apache.reef.tang.Configuration;
import org.apache.reef.util.Optional;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.concurrent.NotThreadSafe;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Simulator for simulating an execution based on functional model.
 * This class manages running environment.
 */
@NotThreadSafe
public abstract class PlanSimulator {
  private static final Logger LOG = LoggerFactory.getLogger(PlanSimulator.class.getName());
  private final String resourceSpecificationString;
  private final String nodeSpecficationString;

  private final ExecutorService serializationExecutorService;
  private final ScheduleSimulator scheduler;
  private final ContainerManageSimulator containerManager;

  public PlanSimulator(final ScheduleSimulator scheduler,
                       final String resourceSpecificationString,
                       final String nodeSpecificationString) throws Exception {
    this.scheduler = scheduler;
    this.resourceSpecificationString = resourceSpecificationString;
    this.nodeSpecficationString = nodeSpecificationString;
    this.containerManager = new ContainerManageSimulator();
    this.serializationExecutorService = new ExecutorServiceSimulator();
    setUpExecutors();
  }

  /**
   * Construct virtual executors for simulation.
   */
  private void setUpExecutors() throws Exception {
    LOG.info("Starts to setup executors");
    long startTimestamp = System.currentTimeMillis();
    this.containerManager.parseNodeSpecificationString(this.nodeSpecficationString);

    final List<Pair<Integer, ResourceSpecification>> resourceSpecs =
      Util.parseResourceSpecificationString(resourceSpecificationString);

    final AtomicInteger executorIdGenerator = new AtomicInteger(0);
    for (Pair<Integer, ResourceSpecification> p : resourceSpecs) {
      for (int i = 0; i < p.left(); i++) {
        final ActiveContext ac = new SimulationEvaluatorActiveContext(executorIdGenerator.getAndIncrement());
        ExecutorRepresenter executorRepresenter = new DefaultExecutorRepresenter(ac.getId(), p.right(),
          new SimulationMessageSender(ac.getId(), this.containerManager), ac, serializationExecutorService, ac.getId());
        this.containerManager.allocateExecutor(executorRepresenter.getExecutorId(), p.right().getCapacity());
        this.scheduler.onExecutorAdded(executorRepresenter);
      }
    }
    LOG.info("Time To setup Executors: " + (System.currentTimeMillis() - startTimestamp));
  }

  /**
   * Reset the instance to its initial state.
   */
  public void reset() throws Exception {
    scheduler.reset();
    containerManager.reset();
    setUpExecutors();
  }

  /**
   * terminates the instances before terminate.
   */
  public void terminate() {
    scheduler.terminate();
    serializationExecutorService.shutdown();
  }

  /**
   * get serializationExecutorService.
   */
  public ExecutorService getSerializationExecutorService() {
    return serializationExecutorService;
  }

  /**
   * get scheduler.
   */
  public ScheduleSimulator getScheduler() {
    return scheduler;
  }

  /**
   * get containerManager.
   */
  public ContainerManageSimulator getContainerManager() {
    return containerManager;
  }

  /**
   * Submits the {@link PhysicalPlan} to Simulator.
   *
   * @param plan               to execute
   * @param maxScheduleAttempt the max number of times this plan/sub-part of the plan should be attempted.
   */
  public abstract void simulate(PhysicalPlan plan, int maxScheduleAttempt) throws Exception;

  /**
   * Evaluator ActiveContext for the Simulation.
   */
  private final class SimulationEvaluatorActiveContext implements ActiveContext {
    private final Integer id;

    /**
     * Default constructor.
     *
     * @param id Evaluator ID.
     */
    SimulationEvaluatorActiveContext(final Integer id) {
      this.id = id;
    }

    @Override
    public void close() {
      // do nothing
    }

    @Override
    public void submitTask(final Configuration taskConf) {
      // do nothing
    }

    @Override
    public void submitContext(final Configuration contextConfiguration) {
      // do nothing
    }

    @Override
    public void submitContextAndService(final Configuration contextConfiguration,
                                        final Configuration serviceConfiguration) {
      // do nothing
    }

    @Override
    public void sendMessage(final byte[] message) {
      // do nothing
    }

    @Override
    public String getEvaluatorId() {
      return getId();
    }

    @Override
    public Optional<String> getParentId() {
      return null;
    }

    @Override
    public EvaluatorDescriptor getEvaluatorDescriptor() {
      return null;
    }

    @Override
    public String getId() {
      return "Evaluator" + id;
    }
  }

  /**
   * ExecutorService for Simulations.
   */
  private final class ExecutorServiceSimulator implements ExecutorService {

    @Override
    public void shutdown() {

    }

    @Override
    public List<Runnable> shutdownNow() {
      return null;
    }

    @Override
    public boolean isShutdown() {
      return false;
    }

    @Override
    public boolean isTerminated() {
      return false;
    }

    @Override
    public boolean awaitTermination(final long timeout, final TimeUnit unit) throws InterruptedException {
      return false;
    }

    @Override
    public <T> Future<T> submit(final Callable<T> task) {
      return null;
    }

    @Override
    public <T> Future<T> submit(final Runnable task, final T result) {
      return null;
    }

    @Override
    public Future<?> submit(final Runnable task) {
      return null;
    }

    @Override
    public <T> List<Future<T>> invokeAll(final Collection<? extends Callable<T>> tasks) throws InterruptedException {
      return null;
    }

    @Override
    public <T> List<Future<T>> invokeAll(final Collection<? extends Callable<T>> tasks,
                                         final long timeout, final TimeUnit unit)
      throws InterruptedException {
      return null;
    }

    @Override
    public <T> T invokeAny(final Collection<? extends Callable<T>> tasks)
      throws InterruptedException, ExecutionException {
      return null;
    }

    @Override
    public <T> T invokeAny(final Collection<? extends Callable<T>> tasks, final long timeout, final TimeUnit unit)
      throws InterruptedException, ExecutionException, TimeoutException {
      return null;
    }

    @Override
    public void execute(final Runnable command) {
      command.run();
    }
  }

  /**
   * MessageSender for Simulations.
   */
  private final class SimulationMessageSender implements MessageSender<ControlMessage.Message> {
    private final String executorId;
    private final ContainerManageSimulator containerManager;

    /**
     * Constructor for the message sender that simply passes on the messages, instead of sending actual messages.
     *
     * @param executorId the simulated executor id of where the message sender communicates from.
     * @param containerManager  the simulation scheduler to communicate with.
     */
    SimulationMessageSender(final String executorId, final ContainerManageSimulator containerManager) {
      this.executorId = executorId;
      this.containerManager = containerManager;
    }

    @Override
    public void send(final ControlMessage.Message message) {
      switch (message.getType()) {
        // Messages sent to the master
        case TaskStateChanged:
        case ExecutorFailed:
        case RunTimePassMessage:
        case MetricMessageReceived:
          break;
        case ScheduleTask:
          final ControlMessage.ScheduleTaskMsg scheduleTaskMsg = message.getScheduleTaskMsg();
          final Task task =
            SerializationUtils.deserialize(scheduleTaskMsg.getTask().toByteArray());
          this.containerManager.onTaskReceived(this.executorId, task);
          break;
        // No metric messaging in simulation.
        case MetricFlushed:
        case RequestMetricFlush:
          break;
        default:
          throw new IllegalMessageException(
            new Exception("This message should not be received by Master or the Executor :" + message.getType()));
      }
    }

    @Override
    public <U> CompletableFuture<U> request(final ControlMessage.Message message) {
      return null;
    }

    @Override
    public void close() {
      // do nothing.
    }
  }
}
