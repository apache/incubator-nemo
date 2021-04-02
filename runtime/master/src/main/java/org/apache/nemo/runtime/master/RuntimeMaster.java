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
package org.apache.nemo.runtime.master;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.protobuf.ByteString;
import org.apache.commons.lang3.SerializationUtils;
import org.apache.nemo.common.Pair;
import org.apache.nemo.common.Util;
import org.apache.nemo.common.exception.ContainerException;
import org.apache.nemo.common.exception.IllegalMessageException;
import org.apache.nemo.common.exception.MetricException;
import org.apache.nemo.common.ir.IRDAG;
import org.apache.nemo.common.ir.executionproperty.ResourceSpecification;
import org.apache.nemo.common.ir.vertex.IRVertex;
import org.apache.nemo.conf.JobConf;
import org.apache.nemo.runtime.common.RuntimeIdManager;
import org.apache.nemo.runtime.common.comm.ControlMessage;
import org.apache.nemo.runtime.common.message.*;
import org.apache.nemo.runtime.common.metric.JobMetric;
import org.apache.nemo.runtime.common.plan.PhysicalPlan;
import org.apache.nemo.runtime.master.metric.MetricManagerMaster;
import org.apache.nemo.runtime.master.metric.MetricMessageHandler;
import org.apache.nemo.runtime.master.metric.MetricStore;
import org.apache.nemo.runtime.master.resource.ContainerManager;
import org.apache.nemo.runtime.master.resource.ExecutorRepresenter;
import org.apache.nemo.runtime.master.scheduler.BatchScheduler;
import org.apache.nemo.runtime.master.scheduler.ExecutorRegistry;
import org.apache.nemo.runtime.master.scheduler.Scheduler;
import org.apache.nemo.runtime.master.servlet.*;
import org.apache.reef.annotations.audience.DriverSide;
import org.apache.reef.driver.context.ActiveContext;
import org.apache.reef.driver.evaluator.AllocatedEvaluator;
import org.apache.reef.driver.evaluator.FailedEvaluator;
import org.apache.reef.tang.Configuration;
import org.apache.reef.tang.annotations.Parameter;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.servlet.ServletHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import java.io.Serializable;
import java.nio.file.Paths;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * (WARNING) Use runtimeMasterThread for all public methods to avoid race conditions.
 * See comments in the {@link Scheduler} for avoiding race conditions.
 * <p>
 * Runtime Master is the central controller of Runtime.
 * Compiler submits an {@link PhysicalPlan} to Runtime Master to execute a job.
 * Runtime Master handles:
 * a) Scheduling the plan with {@link Scheduler}.
 * b) Managing resources with {@link ContainerManager}.
 * c) Managing blocks with {@link BlockManagerMaster}.
 * d) Receiving and sending control messages with {@link MessageEnvironment}.
 * e) Metric using {@link MetricMessageHandler}.
 */
@DriverSide
public final class RuntimeMaster {
  private static final Logger LOG = LoggerFactory.getLogger(RuntimeMaster.class.getName());
  private static final int DAG_LOGGING_PERIOD = 3000;
  private static final int METRIC_ARRIVE_TIMEOUT = 10000;
  private static final int REST_SERVER_PORT = 10101;
  private static final int SPECULATION_CHECKING_PERIOD_MS = 100;

  private final ExecutorService runtimeMasterThread;
  private final ScheduledExecutorService speculativeTaskCloningThread;

  private final Scheduler scheduler;
  private final ContainerManager containerManager;
  private final ExecutorRegistry executorRegistry;
  private final MetricMessageHandler metricMessageHandler;
  private final MessageEnvironment masterMessageEnvironment;
  private final ClientRPC clientRPC;
  private final MetricManagerMaster metricManagerMaster;
  private final PlanStateManager planStateManager;
  // For converting json data. This is a thread safe.
  private final ObjectMapper objectMapper;
  private final String jobId;
  private final String dagDirectory;
  private final Boolean dbEnabled;
  private final String dbAddress;
  private final String dbId;
  private final String dbPassword;
  private final Set<IRVertex> irVertices;
  private final AtomicInteger resourceRequestCount;
  private CountDownLatch metricCountDownLatch;
  // REST API server for web metric visualization ui.
  private final Server metricServer;
  private final MetricStore metricStore;

  /**
   * Constructor.
   *
   * @param scheduler                the scheduler implementation.
   * @param containerManager         the container manager, in charge of the available containers.
   * @param metricMessageHandler     the handler for metric messages.
   * @param masterMessageEnvironment message environment for the runtime master.
   * @param metricManagerMaster      metric manager master.
   * @param clientRPC                the RPC channel to communicate with the client.
   * @param planStateManager         the manager that keeps track of the plan state.
   * @param jobId                    the Job ID, provided by the user.
   * @param dbEnabled                whether or not the DB is enabled, provided by the user.
   * @param dbAddress                the DB Address, provided by the user.
   * @param dbId                     the ID for the given DB.
   * @param dbPassword               the password for the given DB.
   * @param dagDirectory             directory of the DAG to save the json files and metrics into.
   */
  @Inject
  private RuntimeMaster(final Scheduler scheduler,
                        final ContainerManager containerManager,
                        final ExecutorRegistry executorRegistry,
                        final MetricMessageHandler metricMessageHandler,
                        final MessageEnvironment masterMessageEnvironment,
                        final MetricManagerMaster metricManagerMaster,
                        final ClientRPC clientRPC,
                        final PlanStateManager planStateManager,
                        @Parameter(JobConf.JobId.class) final String jobId,
                        @Parameter(JobConf.DBEnabled.class) final Boolean dbEnabled,
                        @Parameter(JobConf.DBAddress.class) final String dbAddress,
                        @Parameter(JobConf.DBId.class) final String dbId,
                        @Parameter(JobConf.DBPasswd.class) final String dbPassword,
                        @Parameter(JobConf.DAGDirectory.class) final String dagDirectory) {
    // We would like to use a single thread for runtime master operations
    // since the processing logic in master takes a very short amount of time
    // compared to the job completion times of executed jobs
    // and keeping it single threaded removes the complexity of multi-thread synchronization.
    this.runtimeMasterThread =
      Executors.newSingleThreadExecutor(runnable -> new Thread(runnable, "RuntimeMaster thread"));

    // Check for speculative execution every second.
    this.speculativeTaskCloningThread = Executors
      .newSingleThreadScheduledExecutor(runnable -> new Thread(runnable, "SpeculativeTaskCloning thread"));
    this.speculativeTaskCloningThread.scheduleAtFixedRate(
      () -> this.runtimeMasterThread.submit(scheduler::onSpeculativeExecutionCheck),
      SPECULATION_CHECKING_PERIOD_MS,
      SPECULATION_CHECKING_PERIOD_MS,
      TimeUnit.MILLISECONDS);

    this.scheduler = scheduler;
    this.containerManager = containerManager;
    this.executorRegistry = executorRegistry;
    this.metricMessageHandler = metricMessageHandler;
    this.masterMessageEnvironment = masterMessageEnvironment;
    this.masterMessageEnvironment
      .setupListener(MessageEnvironment.RUNTIME_MASTER_MESSAGE_LISTENER_ID, new MasterControlMessageReceiver());
    this.clientRPC = clientRPC;
    this.metricManagerMaster = metricManagerMaster;
    this.jobId = jobId;
    this.dagDirectory = dagDirectory;
    this.dbEnabled = dbEnabled;
    this.dbAddress = dbAddress;
    this.dbId = dbId;
    this.dbPassword = dbPassword;
    this.irVertices = new HashSet<>();
    this.resourceRequestCount = new AtomicInteger(0);
    this.objectMapper = new ObjectMapper();
    this.metricServer = startRestMetricServer();
    this.metricStore = MetricStore.getStore();
    this.planStateManager = planStateManager;
    this.metricCountDownLatch = new CountDownLatch(0);
  }

  /**
   * Start Metric Server.
   *
   * @return the metric server.
   */
  private Server startRestMetricServer() {
    final Server server = new Server(REST_SERVER_PORT);

    final ServletHandler servletHandler = new ServletHandler();
    server.setHandler(servletHandler);

    servletHandler.addServletWithMapping(JobMetricServlet.class, "/api/job");
    servletHandler.addServletWithMapping(TaskMetricServlet.class, "/api/task");
    servletHandler.addServletWithMapping(StageMetricServlet.class, "/api/stage");
    servletHandler.addServletWithMapping(AllMetricServlet.class, "/api");
    servletHandler.addServletWithMapping(WebSocketMetricServlet.class, "/api/websocket");

    try {
      server.start();
    } catch (final Exception e) {
      throw new MetricException("Failed to start REST API server: " + e);
    }

    return server;
  }

  /**
   * Record IR DAG related metrics.
   *
   * @param irdag  the IR DAG to record.
   * @param planId the ID of the IR DAG Physical Plan.
   */
  public void recordIRDAGMetrics(final IRDAG irdag, final String planId) {
    metricStore.getOrCreateMetric(JobMetric.class, planId).setIRDAG(irdag);
  }

  /**
   * Flush metrics.
   */
  public void flushMetrics() {
    if (metricCountDownLatch.getCount() == 0) {
      metricCountDownLatch = new CountDownLatch(executorRegistry.getNumberOfRunningExecutors());
      // send metric flush request to all executors
      metricManagerMaster.sendMetricFlushRequest();
    }

    try {
      if (!metricCountDownLatch.await(METRIC_ARRIVE_TIMEOUT, TimeUnit.MILLISECONDS)) {
        LOG.warn("Write Metric before all metric messages arrived.");
      }
    } catch (InterruptedException e) {
      LOG.warn("Waiting Save Metric Process interrupted: ", e);
      // clean up state...
      Thread.currentThread().interrupt();
    }

    // save metric to file
    metricStore.dumpAllMetricToFile(Paths.get(dagDirectory,
      "Metric_" + jobId + "_" + System.currentTimeMillis() + ".json").toString());

    // save metric to database
    if (this.dbEnabled) {
      metricStore.saveOptimizationMetricsToDB(dbAddress, jobId, dbId, dbPassword);
    }
  }

  /**
   * Submits the {@link PhysicalPlan} to Runtime.
   * At now, we are assuming that a single job submit multiple plans.
   *
   * @param plan               to execute
   * @param maxScheduleAttempt the max number of times this plan/sub-part of the plan should be attempted.
   * @return pair of {@link PlanStateManager} and {@link ScheduledExecutorService}
   */
  public Pair<PlanStateManager, ScheduledExecutorService> execute(final PhysicalPlan plan,
                                                                  final int maxScheduleAttempt) {
    final Callable<Pair<PlanStateManager, ScheduledExecutorService>> planExecutionCallable = () -> {
      this.irVertices.addAll(plan.getIdToIRVertex().values());
      try {
        scheduler.schedulePlan(plan, maxScheduleAttempt);
        final ScheduledExecutorService dagLoggingExecutor = scheduleDagLogging();
        return Pair.of(planStateManager, dagLoggingExecutor);
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    };
    try {
      return runtimeMasterThread.submit(planExecutionCallable).get();
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * Terminates the RuntimeMaster.
   */
  public void terminate() {
    // No need to speculate anymore
    speculativeTaskCloningThread.shutdown();

    try {
      // wait for metric flush
      if (!metricCountDownLatch.await(METRIC_ARRIVE_TIMEOUT, TimeUnit.MILLISECONDS)) {
        LOG.warn("Terminating master before all executor terminated messages arrived.");
      }
    } catch (final InterruptedException e) {
      LOG.warn("Waiting executor terminating process interrupted: ", e);
      // clean up state...
      Thread.currentThread().interrupt();
    }

    runtimeMasterThread.execute(() -> {
      scheduler.terminate();
      try {
        masterMessageEnvironment.close();
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
      metricMessageHandler.terminate();
      containerManager.terminate();

      try {
        metricServer.stop();
      } catch (final Exception e) {
        throw new MetricException("Failed to stop rest api server: " + e);
      }
    });

    // Do not shutdown runtimeMasterThread. We need it to clean things up.
  }

  /**
   * Requests a container with resource specification.
   *
   * @param resourceSpecificationString the resource specification.
   */
  public void requestContainer(final String resourceSpecificationString) {
    final Future<?> containerRequestEventResult = runtimeMasterThread.submit(() -> {
      try {
        final List<Pair<Integer, ResourceSpecification>> resourceSpecificationList =  // pair of (# of executors, specs)
          Util.parseResourceSpecificationString(resourceSpecificationString);

        for (final Pair<Integer, ResourceSpecification> resourceSpecification: resourceSpecificationList) {
          resourceRequestCount.getAndAdd(resourceSpecification.left());
          containerManager.requestContainer(resourceSpecification.left(), resourceSpecification.right());
        }

      } catch (final Exception e) {
        throw new ContainerException(e);
      }
    });
    try {
      containerRequestEventResult.get();
    } catch (final Exception e) {
      LOG.error("Exception while requesting for a container: ", e);
      throw new ContainerException(e);
    }
  }

  /**
   * Called when a container is allocated for this runtime.
   * A wrapper function for {@link ContainerManager}.
   *
   * @param executorId            to use for the executor to be launched on this container.
   * @param allocatedEvaluator    to be used as the container.
   * @param executorConfiguration to use for the executor to be launched on this container.
   */
  public void onContainerAllocated(final String executorId,
                                   final AllocatedEvaluator allocatedEvaluator,
                                   final Configuration executorConfiguration) {
    runtimeMasterThread.execute(() ->
      containerManager.onContainerAllocated(executorId, allocatedEvaluator, executorConfiguration));
  }

  /**
   * Called when an executor is launched on a container for this runtime.
   *
   * @param activeContext of the launched executor.
   * @return true if all requested executors have been launched, false otherwise.
   */
  public boolean onExecutorLaunched(final ActiveContext activeContext) {
    final Callable<Boolean> processExecutorLaunchedEvent = () -> {
      final Optional<ExecutorRepresenter> executor = containerManager.onContainerLaunched(activeContext);
      if (executor.isPresent()) {
        scheduler.onExecutorAdded(executor.get());
        return (resourceRequestCount.decrementAndGet() == 0);
      } else {
        return false;
      }
    };

    final boolean eventResult;
    try {
      eventResult = runtimeMasterThread.submit(processExecutorLaunchedEvent).get();
    } catch (final Exception e) {
      throw new ContainerException(e);
    }
    return eventResult;
  }

  /**
   * Called when an executor fails due to container failure on this runtime.
   *
   * @param failedEvaluator that failed.
   */
  public void onExecutorFailed(final FailedEvaluator failedEvaluator) {
    runtimeMasterThread.execute(() -> {
      metricCountDownLatch.countDown();

      // Note that getFailedContextList() can be empty if the failure occurred
      // prior to launching an Executor on the Evaluator.
      failedEvaluator.getFailedContextList().forEach(failedContext -> {
        final String failedExecutorId = failedContext.getId();
        scheduler.onExecutorRemoved(failedExecutorId);
      });

      containerManager.onContainerFailed(failedEvaluator.getId());
    });
  }

  /**
   * Handler for control messages received by Master.
   */
  public final class MasterControlMessageReceiver implements MessageListener<ControlMessage.Message> {
    @Override
    public void onMessage(final ControlMessage.Message message) {
      runtimeMasterThread.execute(() ->
        handleControlMessage(message));
    }

    @Override
    public void onMessageWithContext(final ControlMessage.Message message, final MessageContext messageContext) {
      switch (message.getType()) {
        case RequestBroadcastVariable:
          final Serializable broadcastId =
            SerializationUtils.deserialize(message.getRequestbroadcastVariableMsg().getBroadcastId().toByteArray());
          final Object broadcastVariable = BroadcastManagerMaster.getBroadcastVariable(broadcastId);
          if (broadcastVariable == null) {
            throw new IllegalStateException(broadcastId.toString());
          }
          messageContext.reply(
            ControlMessage.Message.newBuilder()
              .setId(RuntimeIdManager.generateMessageId())
              .setListenerId(MessageEnvironment.RUNTIME_MASTER_MESSAGE_LISTENER_ID)
              .setType(ControlMessage.MessageType.InMasterBroadcastVariable)
              .setBroadcastVariableMsg(ControlMessage.InMasterBroadcastVariableMessage.newBuilder()
                .setRequestId(message.getId())
                // TODO #206: Efficient Broadcast Variable Serialization
                .setVariable(ByteString.copyFrom(SerializationUtils.serialize((Serializable) broadcastVariable)))
                .build())
              .build());
          break;
        default:
          throw new IllegalMessageException(
            new Exception("This message should not be requested to Master :" + message.getType()));
      }
    }
  }

  private void handleControlMessage(final ControlMessage.Message message) {
    switch (message.getType()) {
      case TaskStateChanged:
        final ControlMessage.TaskStateChangedMsg taskStateChangedMsg
          = message.getTaskStateChangedMsg();

        scheduler.onTaskStateReportFromExecutor(taskStateChangedMsg.getExecutorId(),
          taskStateChangedMsg.getTaskId(),
          taskStateChangedMsg.getAttemptIdx(),
          MessageUtils.convertTaskState(taskStateChangedMsg.getState()),
          taskStateChangedMsg.getVertexPutOnHoldId(),
          MessageUtils.convertFailureCause(taskStateChangedMsg.getFailureCause()));
        break;
      case ExecutorFailed:
        // Executor failed due to user code.
        final ControlMessage.ExecutorFailedMsg executorFailedMsg = message.getExecutorFailedMsg();
        final String failedExecutorId = executorFailedMsg.getExecutorId();
        final Exception exception = SerializationUtils.deserialize(executorFailedMsg.getException().toByteArray());
        LOG.error(failedExecutorId + " failed, Stack Trace: ", exception);
        throw new RuntimeException(exception);
      case RunTimePassMessage:
        ((BatchScheduler) scheduler).onRunTimePassMessage(
          message.getRunTimePassMessageMsg().getTaskId(),
          message.getRunTimePassMessageMsg().getEntryList());
        break;
      case MetricMessageReceived:
        final List<ControlMessage.Metric> metricList = message.getMetricMsg().getMetricList();
        metricList.forEach(metric -> metricMessageHandler.onMetricMessageReceived(
            metric.getMetricType(), metric.getMetricId(),
            metric.getMetricField(), metric.getMetricValue().toByteArray()));
        break;
      case ExecutorDataCollected:
        final String serializedData = message.getDataCollected().getData();
        clientRPC.send(ControlMessage.DriverToClientMessage.newBuilder()
          .setType(ControlMessage.DriverToClientMessageType.DataCollected)
          .setDataCollected(ControlMessage.DataCollectMessage.newBuilder().setData(serializedData).build())
          .build());
        break;
      case MetricFlushed:
        metricCountDownLatch.countDown();
        break;
      default:
        throw new IllegalMessageException(
          new Exception("This message should not be received by Master :" + message.getType()));
    }
  }

  /**
   * Schedules a periodic DAG logging thread.
   * TODO #20: RESTful APIs to Access Job State and Metric.
   *
   * @return the scheduled executor service.
   */
  private ScheduledExecutorService scheduleDagLogging() {
    final ScheduledExecutorService dagLoggingExecutor = Executors.newSingleThreadScheduledExecutor();
    dagLoggingExecutor.scheduleAtFixedRate(new Runnable() {
      public void run() {
        flushMetrics();
        planStateManager.storeJSON("periodic");
      }
    }, DAG_LOGGING_PERIOD, DAG_LOGGING_PERIOD, TimeUnit.MILLISECONDS);

    return dagLoggingExecutor;
  }
}
