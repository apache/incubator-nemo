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

import com.google.protobuf.ByteString;
import org.apache.commons.lang3.SerializationUtils;
import org.apache.nemo.common.Pair;
import org.apache.nemo.common.Task;
import org.apache.nemo.common.exception.*;
import org.apache.nemo.common.ir.IRDAG;
import org.apache.nemo.common.ir.vertex.IRVertex;
import org.apache.nemo.common.ir.vertex.executionproperty.ResourcePriorityProperty;
import org.apache.nemo.conf.JobConf;
import org.apache.nemo.common.RuntimeIdManager;
import org.apache.nemo.runtime.common.HDFSUtils;
import org.apache.nemo.runtime.common.comm.ControlMessage;
import org.apache.nemo.runtime.common.plan.PhysicalPlan;
import org.apache.nemo.runtime.common.state.TaskState;
import org.apache.nemo.runtime.master.metric.MetricMessageHandler;
import org.apache.nemo.runtime.master.scheduler.*;
import org.apache.nemo.runtime.master.servlet.*;
import org.apache.nemo.runtime.master.resource.ContainerManager;
import org.apache.nemo.runtime.master.resource.ExecutorRepresenter;
import org.apache.nemo.runtime.master.resource.ResourceSpecification;
import org.apache.nemo.runtime.message.MessageContext;
import org.apache.nemo.runtime.message.MessageEnvironment;
import org.apache.nemo.runtime.message.MessageListener;
import org.apache.nemo.runtime.message.PersistentConnectionToMasterMap;
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

import com.fasterxml.jackson.core.TreeNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import javax.inject.Inject;
import java.io.IOException;
import java.io.Serializable;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import static org.apache.nemo.runtime.common.comm.ControlMessage.MessageType.ResponseOffloadingExecutor;
import static org.apache.nemo.runtime.common.state.TaskState.State.COMPLETE;
import static org.apache.nemo.runtime.common.state.TaskState.State.ON_HOLD;
import static org.apache.nemo.runtime.message.MessageEnvironment.ListenerType.EXECUTOR_MESSAGE_LISTENER_ID;
import static org.apache.nemo.runtime.message.MessageEnvironment.ListenerType.RUNTIME_MASTER_MESSAGE_LISTENER_ID;
import static org.apache.nemo.runtime.message.MessageEnvironment.ListenerType.YARN_OFFLOADING_EXECUTOR_REQUEST_ID;

/**
 * (WARNING) Use runtimeMasterThread for all public methods to avoid race conditions.
 * See comments in the {@link Scheduler} for avoiding race conditions.
 *
 * Runtime Master is the central controller of Runtime.
 * Compiler submits an {@link PhysicalPlan} to Runtime Master to execute a job.
 * Runtime Master handles:
 *    a) Scheduling the plan with {@link Scheduler}.
 *    b) Managing resources with {@link ContainerManager}.
 *    c) Managing blocks with {@link BlockManagerMaster}.
 *    d) Receiving and sending control messages with {@link MessageEnvironment}.
 *    e) Metric using {@link MetricMessageHandler}.
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
  private final MessageEnvironment masterMessageEnvironment;
  private final ClientRPC clientRPC;
  private final PlanStateManager planStateManager;
  // For converting json data. This is a thread safe.
  private final ObjectMapper objectMapper;
  private final Set<IRVertex> irVertices;
  private final AtomicInteger resourceRequestCount;
  private CountDownLatch metricCountDownLatch;

  private final TaskScheduledMapMaster taskScheduledMap;

  private final ExecutorRegistry executorRegistry;
  private final PendingTaskCollectionPointer pendingTaskCollectionPointer;
  private final TaskDispatcher taskDispatcher;

  private final String resourceSpecificationString;

  private final OffloadingWorkerManager offloadingWorkerManager;

  private final PersistentConnectionToMasterMap toMaster;

  @Inject
  private RuntimeMaster(final Scheduler scheduler,
                        final TaskScheduledMapMaster taskScheduledMap,
                        final ContainerManager containerManager,
                        final MessageEnvironment masterMessageEnvironment,
                        final PersistentConnectionToMasterMap toMaster,
                        final ClientRPC clientRPC,
                        final OffloadingWorkerManager offloadingWorkerManager,
                        final PlanStateManager planStateManager,
                        @Parameter(JobConf.ExecutorJSONContents.class) final String resourceSpecificationString,
                        final PendingTaskCollectionPointer pendingTaskCollectionPointer,
                        final TaskDispatcher taskDispatcher,
                        final ExecutorRegistry executorRegistry) throws IOException {
    // We would like to use a single thread for runtime master operations
    // since the processing logic in master takes a very short amount of time
    // compared to the job completion times of executed jobs
    // and keeping it single threaded removes the complexity of multi-thread synchronization.
    this.toMaster = toMaster;
    this.offloadingWorkerManager = offloadingWorkerManager;
    this.resourceSpecificationString = resourceSpecificationString;
    this.executorRegistry = executorRegistry;
    this.taskDispatcher = taskDispatcher;
    this.pendingTaskCollectionPointer = pendingTaskCollectionPointer;
    this.runtimeMasterThread =
        Executors.newSingleThreadExecutor(runnable -> {
          final Thread t = new Thread(runnable, "RuntimeMaster thread");
          t.setUncaughtExceptionHandler(new Thread.UncaughtExceptionHandler() {
              @Override
              public void uncaughtException(Thread t, Throwable e) {
                e.printStackTrace();
                terminate();
                clientRPC.send(ControlMessage.DriverToClientMessage.newBuilder()
                  .setType(ControlMessage.DriverToClientMessageType.DriverShutdowned).build());
              }
            });
          return t;
        });

    this.taskScheduledMap = taskScheduledMap;

    // Check for speculative execution every second.
    this.speculativeTaskCloningThread = Executors
      .newSingleThreadScheduledExecutor(runnable -> new Thread(runnable, "SpeculativeTaskCloning thread"));
    this.speculativeTaskCloningThread.scheduleAtFixedRate(
      () -> this.runtimeMasterThread.submit(scheduler::onSpeculativeExecutionCheck),
      SPECULATION_CHECKING_PERIOD_MS,
      SPECULATION_CHECKING_PERIOD_MS,
      TimeUnit.MILLISECONDS);

    HDFSUtils.createStateDirIfNotExistsAndDelete();

    this.scheduler = scheduler;
    this.containerManager = containerManager;
    this.masterMessageEnvironment = masterMessageEnvironment;
    this.masterMessageEnvironment
        .setupListener(RUNTIME_MASTER_MESSAGE_LISTENER_ID, new MasterControlMessageReceiver());
    this.clientRPC = clientRPC;
    this.irVertices = new HashSet<>();
    this.resourceRequestCount = new AtomicInteger(0);
    this.objectMapper = new ObjectMapper();
    this.planStateManager = planStateManager;
  }

  public ExecutorService getRuntimeMasterThread() {
    return runtimeMasterThread;
  }

  /**
   * Start Metric Server.
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

  public void recordIRDAGMetrics(final IRDAG irdag, final String planId) {
    //metricStore.getOrCreateMetric(JobMetric.class, planId).setIRDAG(irdag);
  }

  /**
   * Flush metrics.
   */
  public void flushMetrics() {
    // send metric flush request to all executors
    /*
    metricManagerMaster.sendMetricFlushRequest();

    metricStore.dumpAllMetricToFile(Paths.get(dagDirectory,
      "Metric_" + jobId + "_" + System.currentTimeMillis() + ".json").toString());
    metricStore.saveOptimizationMetricsToDB(dbAddress, dbId, dbPassword);
    */
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
  private boolean terminated = false;
  public void terminate() {
    if (!terminated) {
      terminated = true;
      // No need to speculate anymore
      LOG.info("RuntimeMaster termiate called");
      speculativeTaskCloningThread.shutdown();

      planStateManager.shutdown();

      if (metricCountDownLatch != null) {
        try {
          // wait for metric flush
          if (!metricCountDownLatch.await(METRIC_ARRIVE_TIMEOUT, TimeUnit.MILLISECONDS)) {
            LOG.warn("Terminating master before all executor terminated messages arrived.");
          }
        } catch (final InterruptedException e) {
          LOG.warn("Waiting executor terminating process interrupted: " + e);
          // clean up state...
          Thread.currentThread().interrupt();
        }
      }
      runtimeMasterThread.execute(() -> {
        scheduler.terminate();
        try {
          masterMessageEnvironment.close();
        } catch (Exception e) {
          throw new RuntimeException(e);
        }
        containerManager.terminate();
      });
    }
    // Do not shutdown runtimeMasterThread. We need it to clean things up.
  }

  /**
   * Requests a container with resource specification.
   *
   * @param resourceSpecificationString the resource specification.
   */
  public void requestContainer(final String resourceSpecificationString,
                               final boolean onlyCompute,
                               final boolean offloading,
                               final String name,
                               final int num) {
    final Future<?> containerRequestEventResult = runtimeMasterThread.submit(() -> {
      try {
        final TreeNode jsonRootNode = objectMapper.readTree(resourceSpecificationString);

        LOG.info("Request container {} / {} / {} / {}",
          resourceSpecificationString, onlyCompute, offloading, name);

        for (int i = 0; i < jsonRootNode.size(); i++) {
          final TreeNode resourceNode = jsonRootNode.get(i);
          final String type = resourceNode.get("type").traverse().nextTextValue();
          final int memory = resourceNode.get("memory_mb").traverse().getIntValue();
          final int capacity = resourceNode.get("capacity").traverse().getIntValue();
          final int slot = resourceNode.get("slot").traverse().getIntValue();
          int executorNum = 0;
          if (num > 0) {
            executorNum = num;
          } else {
            executorNum = resourceNode.path("num").traverse().nextIntValue(1);
          }
          final int poisonSec = resourceNode.path("poison_sec").traverse().nextIntValue(-1);

          LOG.info("Creating type {}, mem {}. capa: {}, slot: {}, num: {}",
            type, memory, capacity, slot, executorNum);

          if (offloading) {
            if (type.equals(ResourcePriorityProperty.OFFLOAD)) {
              containerManager.requestContainer(executorNum,
                new ResourceSpecification(type,
                  capacity, slot, memory, poisonSec), name);
            }
          } else {
            if (onlyCompute) {
              if (type.equals(ResourcePriorityProperty.COMPUTE)) {
                resourceRequestCount.getAndAdd(executorNum);
                containerManager.requestContainer(executorNum,
                  new ResourceSpecification(type, capacity, slot, memory, poisonSec), name);
              }
            } else {
              if (!type.equals(ResourcePriorityProperty.OFFLOAD)) {
                resourceRequestCount.getAndAdd(executorNum);
                containerManager.requestContainer(executorNum,
                  new ResourceSpecification(type, capacity, slot, memory, poisonSec), name);
              }
            }
          }
        }
        metricCountDownLatch = new CountDownLatch(resourceRequestCount.get());
      } catch (final Exception e) {
        e.printStackTrace();
        throw new ContainerException(e);
      }
    });
    try {
      containerRequestEventResult.get();
    } catch (final Exception e) {
      e.printStackTrace();
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
    LOG.info("Executor Launched {} " + activeContext.getId());

    if (!activeContext.getId().contains("offloading")) {
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
    } else {
      return true;
    }
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

  public void createScalingExecutor(final int num) {
    // TODO-1: todo
    /*
    LOG.info("Executor Launched {} " + activeContext.getId());

    if (!activeContext.getId().contains("offloading")) {
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
    } else {
      return true;
    }
    */
  }


  public void createOffloadingExecutor(final int num) {
    LOG.info("Create offloading executor");
    executorRegistry.viewExecutors(executors -> {
      executors.forEach(executor -> {
        if (!executor.getContainerType().equals(ResourcePriorityProperty.SOURCE)) {
          LOG.info("Create offloading executor for executor {}", executor.getExecutorId());
          executor.sendControlMessage(ControlMessage.Message.newBuilder()
            .setId(RuntimeIdManager.generateMessageId())
            .setListenerId(EXECUTOR_MESSAGE_LISTENER_ID.ordinal())
            .setType(ControlMessage.MessageType.CreateOffloadingExecutor)
            .setSetNum(num)
            .build());
        }
      });
    });
  }

  public void throttleSource(final int num) {
    LOG.info("Throttle source rate {}", num);
    executorRegistry.viewExecutors(executors -> {
      executors.forEach(executor -> {
        if (executor.getContainerType().equals(ResourcePriorityProperty.SOURCE)) {
          LOG.info("Throttle source for executor {}", executor.getExecutorId());
          executor.sendControlMessage(ControlMessage.Message.newBuilder()
            .setId(RuntimeIdManager.generateMessageId())
            .setListenerId(EXECUTOR_MESSAGE_LISTENER_ID.ordinal())
            .setType(ControlMessage.MessageType.ThrottleSource)
            .setSetNum(num)
            .build());
        }
      });
    });
  }

  public void deoffloadTask(final int num,
                            final int stageId) {
    LOG.info("Deoffloading tasks {}", num);
    executorRegistry.viewExecutors(executors -> {
      executors.forEach(executor -> {
        if (executor.getContainerType().equals(ResourcePriorityProperty.COMPUTE)) {
          LOG.info("Deoffloading task for executor {}", executor.getExecutorId());
          executor.sendControlMessage(ControlMessage.Message.newBuilder()
            .setId(RuntimeIdManager.generateMessageId())
            .setListenerId(EXECUTOR_MESSAGE_LISTENER_ID.ordinal())
            .setType(ControlMessage.MessageType.DeoffloadingTask)
            .setOffloadingTaskMsg(ControlMessage.OffloadingTaskMessage.newBuilder()
              .setNumOffloadingTask(num)
              .setOffloadingStage(stageId)
              .build())
            .build());
        }
      });
    });
  }

  public void sendTaskToLambda() {
    offloadingWorkerManager.sendTaskToLambda();
  }

  public void activateLambda() {
    offloadingWorkerManager.activateAllWorkers();
  }

  public void deactivateLambda() {
    offloadingWorkerManager.deactivateAllWorkers();
  }

  public void invokePartialOffloading() {
    activateLambda();

    executorRegistry.viewExecutors(executors -> {
      executors.forEach(executor -> {
        if (executor.getContainerType().equals(ResourcePriorityProperty.COMPUTE)) {
          LOG.info("Invoke partila offloading for executor {}", executor.getExecutorId());
          executor.sendControlMessage(ControlMessage.Message.newBuilder()
            .setId(RuntimeIdManager.generateMessageId())
            .setListenerId(EXECUTOR_MESSAGE_LISTENER_ID.ordinal())
            .setType(ControlMessage.MessageType.InvokePartialOffloading)
            .build());
        }
      });
    });
  }

  public void sendBursty(final int num) {
    activateLambda();

    LOG.info("Offloading bursty tasks {}", num);
    executorRegistry.viewExecutors(executors -> {
      executors.forEach(executor -> {
        if (executor.getContainerType().equals(ResourcePriorityProperty.COMPUTE)) {
          LOG.info("Offloading task for executor {}", executor.getExecutorId());
          executor.sendControlMessage(ControlMessage.Message.newBuilder()
            .setId(RuntimeIdManager.generateMessageId())
            .setListenerId(EXECUTOR_MESSAGE_LISTENER_ID.ordinal())
            .setType(ControlMessage.MessageType.SendBursty)
            .setOffloadingTaskMsg(ControlMessage.OffloadingTaskMessage.newBuilder()
              .setNumOffloadingTask(num)
              .build())
            .build());
        }
      });
    });
  }

  public void finishBursty(final int num) {
    LOG.info("Finish bursty tasks {}", num);
    executorRegistry.viewExecutors(executors -> {
      executors.forEach(executor -> {
        if (executor.getContainerType().equals(ResourcePriorityProperty.COMPUTE)) {
          LOG.info("Deoffloading task for executor {}", executor.getExecutorId());
          executor.sendControlMessage(ControlMessage.Message.newBuilder()
            .setId(RuntimeIdManager.generateMessageId())
            .setListenerId(EXECUTOR_MESSAGE_LISTENER_ID.ordinal())
            .setType(ControlMessage.MessageType.FinishBursty)
            .setSetNum(num)
            .build());
        }
      });
    });

    try {
      Thread.sleep(500);
    } catch (InterruptedException e) {
      e.printStackTrace();
    }

    deactivateLambda();
  }


  public void offloadTask(final int num,
                          final int stageId) {
    LOG.info("Offloading tasks {}", num);
    executorRegistry.viewExecutors(executors -> {
      executors.forEach(executor -> {
        if (executor.getContainerType().equals(ResourcePriorityProperty.COMPUTE)) {
          LOG.info("Offloading task for executor {}", executor.getExecutorId());
          executor.sendControlMessage(ControlMessage.Message.newBuilder()
            .setId(RuntimeIdManager.generateMessageId())
            .setListenerId(EXECUTOR_MESSAGE_LISTENER_ID.ordinal())
            .setType(ControlMessage.MessageType.OffloadingTask)
            .setOffloadingTaskMsg(ControlMessage.OffloadingTaskMessage.newBuilder()
              .setNumOffloadingTask(num)
              .setOffloadingStage(stageId)
              .build())
            .build());
        }
      });
    });
  }

  private final Map<String, Pair<Integer, ExecutorRepresenter>> responsePendingMap = new HashMap<>();

  public boolean isOffloadingExecutorEvaluator() {
    synchronized (responsePendingMap) {
      return !responsePendingMap.isEmpty();
    }
  }

  public Pair<String, Integer> getOffloadingExecutorPort(final String hostAddres) {
    synchronized (responsePendingMap) {

      final String name = responsePendingMap.keySet().iterator().next();

      // send response
      LOG.info("Send response for offloading executor " + name + ", " + hostAddres);
      toMaster.getMessageSender(YARN_OFFLOADING_EXECUTOR_REQUEST_ID)
        .send(ControlMessage.Message.newBuilder()
          .setId(RuntimeIdManager.generateMessageId())
          .setListenerId(YARN_OFFLOADING_EXECUTOR_REQUEST_ID.ordinal())
          .setType(ResponseOffloadingExecutor)
          .setRegisteredExecutor(name + "," + hostAddres)
          .build());

      return Pair.of(name, responsePendingMap.remove(name).left());
    }
  }

  /**
   * Handler for control messages received by Master.
   */
  public final class MasterControlMessageReceiver implements MessageListener<ControlMessage.Message> {
    @Override
    public void onMessage(final ControlMessage.Message message) {
      switch (message.getType()) {
        case RequestOffloadingExecutor: {
          final ControlMessage.RequestOffloadingExecutorMessage m = message.getRequestOffloadingExecutorMsg();

          try {
            LOG.info("Receive requestOffloadingExecutor " + m.getExecutorId()
              + ", " + m.getName() + ", " + m.getPort());

            LOG.info("hoho");

            final ExecutorRepresenter re = executorRegistry.getExecutorRepresentor(m.getExecutorId());

            LOG.info("Get executor representer " + m.getExecutorId()
              + ", " + m.getName() + ", " + m.getPort() + ", " + re.getExecutorId());

            synchronized (responsePendingMap) {
              responsePendingMap.put(m.getName(), Pair.of((int) m.getPort(), re));
            }

            LOG.info("Put responsePendingMap " + m.getExecutorId()
              + ", " + m.getName() + ", " + m.getPort() + ", " + re.getExecutorId());

            requestContainer(resourceSpecificationString,
              true,
              true, m.getName(), 1);
            //});
          } catch (final Exception e) {
            e.printStackTrace();
            throw new RuntimeException(e);
          }
          break;
        }
        default:
          runtimeMasterThread.execute(() ->
            handleControlMessage(message));
      }
    }

    @Override
    public void onMessageWithContext(final ControlMessage.Message message, final MessageContext messageContext) {
      switch (message.getType()) {
        case CurrentExecutor: {
          executorRegistry.viewExecutors(executors -> {
            final Set<String> executorIds =
              executors.stream().map(e -> e.getExecutorId()).collect(Collectors.toSet());
            messageContext.reply(
              ControlMessage.Message.newBuilder()
                .setId(RuntimeIdManager.generateMessageId())
                .setListenerId(EXECUTOR_MESSAGE_LISTENER_ID.ordinal())
                .setType(ControlMessage.MessageType.CurrentExecutor)
                .addAllCurrExecutors(executorIds)
                .build());
          });
          break;
        }
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
              .setListenerId(RUNTIME_MASTER_MESSAGE_LISTENER_ID.ordinal())
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
      case StopTaskDone: {
        getRuntimeMasterThread().execute(() -> {
          final ControlMessage.StopTaskDoneMessage stopTaskDone = message.getStopTaskDoneMsg();
          LOG.info("Receive stop task done message " + stopTaskDone.getTaskId() + ", " + stopTaskDone.getExecutorId());
          final ExecutorRepresenter executorRepresenter =
            executorRegistry.getExecutorRepresentor(stopTaskDone.getExecutorId());
          executorRepresenter.onTaskExecutionStop(stopTaskDone.getTaskId());
          final Task task = taskScheduledMap.removeTask(stopTaskDone.getTaskId());
          LOG.info("Change task state to READY " + stopTaskDone.getTaskId());
          planStateManager.onTaskStateChanged(stopTaskDone.getTaskId(), TaskState.State.READY);
          pendingTaskCollectionPointer.addTask(task);
          taskDispatcher.onNewPendingTaskCollectionAvailable();
        });
        break;
      }
      case TaskStateChanged:
        final ControlMessage.TaskStateChangedMsg taskStateChangedMsg
            = message.getTaskStateChangedMsg();

        scheduler.onTaskStateReportFromExecutor(taskStateChangedMsg.getExecutorId(),
            taskStateChangedMsg.getTaskId(),
            taskStateChangedMsg.getAttemptIdx(),
            convertTaskState(taskStateChangedMsg.getState()),
            taskStateChangedMsg.getVertexPutOnHoldId(),
            convertFailureCause(taskStateChangedMsg.getFailureCause()));
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
        /*
        metricList.forEach(metric ->
            metricMessageHandler.onMetricMessageReceived(
                metric.getMetricType(), metric.getMetricId(),
                metric.getMetricField(), metric.getMetricValue().toByteArray()));
                */
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
      case LocalRelayServerInfo: {
        final ControlMessage.LocalRelayServerInfoMessage msg = message.getLocalRelayServerInfoMsg();

        LOG.info("Receive local relay server info for {}", msg.getExecutorId());
        taskScheduledMap.setRelayServerInfo(msg.getExecutorId(),
          msg.getAddress(), msg.getPort());
        break;
      }
      case LocalExecutorAddressInfo: {
        final ControlMessage.LocalExecutorAddressInfoMessage msg = message.getLocalExecutorAddressInfoMsg();

        LOG.info("Receive local executor address info for {}", msg.getExecutorId());
        taskScheduledMap.setExecutorAddressInfo(msg.getExecutorId(),
          msg.getAddress(), msg.getPort());
        break;
      }
      default:
        throw new IllegalMessageException(
            new Exception("This message should not be received by Master :" + message.getType()));
    }
  }

  private static TaskState.State convertTaskState(final ControlMessage.TaskStateFromExecutor state) {
    switch (state) {
      case READY:
        return TaskState.State.READY;
      case EXECUTING:
        return TaskState.State.EXECUTING;
      case COMPLETE:
        return COMPLETE;
      case FAILED_RECOVERABLE:
        return TaskState.State.SHOULD_RETRY;
      case FAILED_UNRECOVERABLE:
        return TaskState.State.FAILED;
      case ON_HOLD:
        return ON_HOLD;
      default:
        throw new UnknownExecutionStateException(new Exception("This TaskState is unknown: " + state));
    }
  }

  private TaskState.RecoverableTaskFailureCause convertFailureCause(
      final ControlMessage.RecoverableFailureCause cause) {
    switch (cause) {
      case InputReadFailure:
        return TaskState.RecoverableTaskFailureCause.INPUT_READ_FAILURE;
      case OutputWriteFailure:
        return TaskState.RecoverableTaskFailureCause.OUTPUT_WRITE_FAILURE;
      default:
        throw new UnknownFailureCauseException(
            new Throwable("The failure cause for the recoverable failure is unknown"));
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
        planStateManager.storeJSON("periodic");
      }
    }, DAG_LOGGING_PERIOD, DAG_LOGGING_PERIOD, TimeUnit.MILLISECONDS);

    return dagLoggingExecutor;
  }
}
