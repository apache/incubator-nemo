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
import org.apache.nemo.conf.EvalConf;
import org.apache.nemo.conf.JobConf;
import org.apache.nemo.common.RuntimeIdManager;
import org.apache.nemo.offloading.common.EventHandler;
import org.apache.nemo.runtime.common.HDFSUtils;
import org.apache.nemo.runtime.common.comm.ControlMessage;
import org.apache.nemo.runtime.common.plan.PhysicalPlan;
import org.apache.nemo.common.TaskState;
import org.apache.nemo.runtime.master.lambda.LambdaContainerManager;
import org.apache.nemo.runtime.master.metric.MetricMessageHandler;
import org.apache.nemo.runtime.master.scheduler.*;
import org.apache.nemo.runtime.master.servlet.*;
import org.apache.nemo.runtime.master.resource.ContainerManager;
import org.apache.nemo.runtime.master.resource.DefaultExecutorRepresenterImpl;
import org.apache.nemo.runtime.master.resource.ResourceSpecification;
import org.apache.nemo.runtime.message.MessageContext;
import org.apache.nemo.runtime.message.MessageEnvironment;
import org.apache.nemo.runtime.message.MessageListener;
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

import static org.apache.nemo.common.TaskState.State.COMPLETE;
import static org.apache.nemo.common.TaskState.State.ON_HOLD;
import static org.apache.nemo.common.ir.vertex.executionproperty.ResourcePriorityProperty.COMPUTE;
import static org.apache.nemo.common.ir.vertex.executionproperty.ResourcePriorityProperty.LAMBDA;
import static org.apache.nemo.common.ir.vertex.executionproperty.ResourcePriorityProperty.OFFLOAD;
import static org.apache.nemo.runtime.message.MessageEnvironment.ListenerType.EXECUTOR_MESSAGE_LISTENER_ID;
import static org.apache.nemo.runtime.message.MessageEnvironment.ListenerType.RUNTIME_MASTER_MESSAGE_LISTENER_ID;

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
  private final ExecutorService requestContainerThread;
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

  // private final OffloadingWorkerManager offloadingWorkerManager;

  private final LambdaContainerManager lambdaContainerManager;
  private final EvalConf evalConf;
  private final PairStageTaskManager pairStageTaskManager;

  @Inject
  private RuntimeMaster(final Scheduler scheduler,
                        final TaskScheduledMapMaster taskScheduledMap,
                        final ContainerManager containerManager,
                        final MessageEnvironment masterMessageEnvironment,
                        final ClientRPC clientRPC,
   //                     final OffloadingWorkerManager offloadingWorkerManager,
                        final PlanStateManager planStateManager,
                        @Parameter(JobConf.ExecutorJSONContents.class) final String resourceSpecificationString,
                        final PendingTaskCollectionPointer pendingTaskCollectionPointer,
                        final TaskDispatcher taskDispatcher,
                        final EvalConf evalConf,
                        final PairStageTaskManager pairStageTaskManager,
                        final LambdaContainerManager lambdaContainerManager,
                        final InMasterControlMessageQueue inMasterControlMessageQueue,
                        final ExecutorRegistry executorRegistry) throws IOException {
    // We would like to use a single thread for runtime master operations
    // since the processing logic in master takes a very short amount of time
    // compared to the job completion times of executed jobs
    // and keeping it single threaded removes the complexity of multi-thread synchronization.
    this.lambdaContainerManager = lambdaContainerManager;
    // this.offloadingWorkerManager = offloadingWorkerManager;
    this.resourceSpecificationString = resourceSpecificationString;
    this.executorRegistry = executorRegistry;
    this.taskDispatcher = taskDispatcher;
    this.evalConf = evalConf;
    this.pairStageTaskManager = pairStageTaskManager;
    this.pendingTaskCollectionPointer = pendingTaskCollectionPointer;
    this.requestContainerThread = Executors.newCachedThreadPool();
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

  private ResourceSpecification getLambdaResourceSpec(
    final String resourceSpecificationString,
    final int num) {
    try {
      final TreeNode jsonRootNode = objectMapper.readTree(resourceSpecificationString);
      for (int i = 0; i < jsonRootNode.size(); i++) {
        final TreeNode resourceNode = jsonRootNode.get(i);
        final String type = resourceNode.get("type").traverse().nextTextValue();

        if (type.equals(LAMBDA)) {
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

          return  new ResourceSpecification(type,
            capacity, slot, memory, poisonSec);
        }
      }
    } catch (final Exception e) {
      e.printStackTrace();
    }

    throw new RuntimeException("Cannot find offload type resource spec");
  }

  public void triggerConditionalRouting(final boolean partial,
                                        final double percent) {
    LOG.info("Trigger conditional routing {} / {}", partial, percent);
    executorRegistry.viewExecutors(executors -> {
      executors.forEach(executor -> {
        executor.sendControlMessage(ControlMessage.Message.newBuilder()
          .setId(RuntimeIdManager.generateMessageId())
          .setListenerId(EXECUTOR_MESSAGE_LISTENER_ID.ordinal())
          .setType(ControlMessage.MessageType.ConditionalRouting)
          .setConditionalRoutingMsg(ControlMessage.ConditionalRoutingMessage.newBuilder()
            .setPercent(percent)
            .setToPartial(partial)
            .build())
          .build());
      });
    });
  }

  /**
   * Requests a container with resource specification.
   *
   * @param resourceSpecificationString the resource specification.
   */
  public void requestContainer(final String resourceSpecificationString,
                               final boolean createWithLambda,
                               final boolean createOnlyLambda,
                               final String name,
                               final int num) {

    if (createWithLambda && createOnlyLambda) {
      throw new RuntimeException("Invalid request " + createWithLambda +  " " + createOnlyLambda);
    }

    LOG.info("Requesting container 11 {}", name);
    final Future<?> containerRequestEventResult = requestContainerThread.submit(() -> {
      LOG.info("Requesting container 22 {}", name);
      try {
        final TreeNode jsonRootNode = objectMapper.readTree(resourceSpecificationString);

        LOG.info("Request container {} / {} / {} / {}",
          resourceSpecificationString, createWithLambda, createOnlyLambda, name);

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

          if (createWithLambda) {
            if (type.equals(LAMBDA)) {
              requestLambdaContainer(executorNum, true, capacity, slot, memory);
            } else {
              resourceRequestCount.getAndAdd(executorNum);
              containerManager.requestContainer(executorNum,
                new ResourceSpecification(type, capacity, slot, memory, poisonSec), name);
            }
          } else if (createOnlyLambda) {
            if (type.equals(LAMBDA)) {
              requestLambdaContainer(executorNum, true, capacity, slot, memory);
            }
          } else {
            if (!type.equals(LAMBDA)) {
              resourceRequestCount.getAndAdd(executorNum);
              containerManager.requestContainer(executorNum,
                new ResourceSpecification(type, capacity, slot, memory, poisonSec), name);
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
      requestContainerThread.execute(() ->
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

  public void requestLambdaContainer(final int num,
                                     final boolean resourceTypeLambda,
                                     final int capacity,
                                     final int slot,
                                     final int memory) {
    resourceRequestCount.getAndAdd(num);
    requestContainerThread.execute(() -> {
      final List<Future<ExecutorRepresenter>> list =
        lambdaContainerManager.createLambdaContainer(num, resourceTypeLambda,
          capacity, slot, memory);

      for (final Future<ExecutorRepresenter> future : list) {
        final Callable<Boolean> processExecutorLaunchedEvent = () -> {
          final ExecutorRepresenter executor = future.get();
          scheduler.onExecutorAdded(executor);
          return (resourceRequestCount.decrementAndGet() == 0);
        };

        final boolean eventResult;
        try {
          eventResult = processExecutorLaunchedEvent.call();
          // eventResult = runtimeMasterThread.submit(processExecutorLaunchedEvent).get();
        } catch (final Exception e) {
          throw new ContainerException(e);
        }
      }
    });
  }

  public void stopLambdaContainer(final int num) {
    lambdaContainerManager.stopLambdaContainer(num);
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
    // offloadingWorkerManager.sendTaskToLambda();
  }

  public void activateLambda() {
   //  offloadingWorkerManager.activateAllWorkers();
  }

  public void deactivateLambda() {
   // offloadingWorkerManager.deactivateAllWorkers();
    // 1) deactivate tasks
    executorRegistry.viewExecutors(executors -> {
      executors.forEach(executor -> {
        if (executor.getContainerType().equals(ResourcePriorityProperty.LAMBDA)) {
          LOG.info("Deactivate lambda task for executor {}", executor.getExecutorId());
          executor.sendControlMessage(ControlMessage.Message.newBuilder()
            .setId(RuntimeIdManager.generateMessageId())
            .setListenerId(EXECUTOR_MESSAGE_LISTENER_ID.ordinal())
            .setType(ControlMessage.MessageType.DeactivateLambdaTask)
            .build());
        }
      });
    });

    // 2) TODO: deactivate lambda workers
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

  private final Set<String> prevRedirectionTasks = new HashSet<>();

  public void redirectionToLambda(final int num,
                                  final String stageId) {
    final Map<String, String> scheduledMap = taskScheduledMap.getTaskExecutorIdMap();
    scheduledMap.entrySet().stream()
      .filter(entry -> RuntimeIdManager.getStageIdFromTaskId(entry.getKey())
        .equals(stageId)
      && !prevRedirectionTasks.contains(entry.getKey()))
      .limit(num)
      .forEach(entry -> {
        final String taskId = entry.getKey();
        final String executorId = entry.getValue();
        final ExecutorRepresenter executor = executorRegistry.getExecutorRepresentor(executorId);

        LOG.info("Redirection VM task {} for executor {}", taskId, executor.getExecutorId());

        prevRedirectionTasks.add(taskId);

        executor.sendControlMessage(ControlMessage.Message.newBuilder()
          .setId(RuntimeIdManager.generateMessageId())
          .setListenerId(EXECUTOR_MESSAGE_LISTENER_ID.ordinal())
          .setType(ControlMessage.MessageType.RoutingDataToLambda)
          .setStopTaskMsg(ControlMessage.StopTaskMessage.newBuilder()
            .setTaskId(taskId)
            .build())
          .build());
      });

    LOG.info("Redirection tasks {} / stage {}", num, stageId);
  }

  public void redirectionDoneToLambda(final int num,
                                      final String stageId) {
    final Iterator<String> iterator = prevRedirectionTasks.iterator();


    int count = 0;
    while (iterator.hasNext()) {
      final String taskId = iterator.next();
      if (RuntimeIdManager.getStageIdFromTaskId(taskId).equals(stageId) && count < num) {
        count += 1;
        // find pair task
        final String pairTask = pairStageTaskManager.getPairTaskEdgeId(taskId).left();
        final String pairExecutorId = taskScheduledMap.getTaskExecutorIdMap().get(pairTask);

        LOG.info("Redirection Lambda task {} for executor {}", pairTask, pairExecutorId);
        final ExecutorRepresenter executor = executorRegistry.getExecutorRepresentor(pairExecutorId);

        iterator.remove();

        executor.sendControlMessage(ControlMessage.Message.newBuilder()
          .setId(RuntimeIdManager.generateMessageId())
          .setListenerId(EXECUTOR_MESSAGE_LISTENER_ID.ordinal())
          .setType(ControlMessage.MessageType.RoutingDataToLambda)
          .setStopTaskMsg(ControlMessage.StopTaskMessage.newBuilder()
            .setTaskId(pairTask)
            .build())
          .build());
      }
    }

    LOG.info("Redirection lambda tasks {} / stage {}", num, stageId);
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

  private final Map<String, Pair<Integer, EventHandler<String>>> responsePendingMap = new HashMap<>();

  public boolean isOffloadingExecutorEvaluator() {
    synchronized (responsePendingMap) {
      return !responsePendingMap.isEmpty();
    }
  }

  public void requestOffloadingExecutor(final int port,
                                        final String name,
                                        final String executorId,
                                        final String containerType,
                                        final int capacity,
                                        final int slot,
                                        final int memory,
                                        final EventHandler<String> requestHandler) {

    try {
      LOG.info("Receive requestOffloadingExecutor " + executorId
        + ", " + name + ", " + port + ", type " + containerType);

      synchronized (responsePendingMap) {
        responsePendingMap.put(name, Pair.of((int) port, requestHandler));
      }

      LOG.info("Try to request {}/{}/{}", name, executorId, containerType);

      containerManager.requestContainer(1,
              new ResourceSpecification(containerType, capacity, slot, memory, -1), name);

    } catch (final Exception e) {
      e.printStackTrace();
      throw new RuntimeException(e);
    }
  }

  public Pair<String, Integer> getOffloadingExecutorPort(final String hostAddres) {
    synchronized (responsePendingMap) {

      final String name = responsePendingMap.keySet().iterator().next();

      // send response
      LOG.info("Send response for offloading executor " + name + ", " + hostAddres);
      responsePendingMap.get(name).right().onNext(hostAddres);

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
                .setId(messageContext.getRequestId())
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
              .setId(messageContext.getRequestId())
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

  private final long st = System.currentTimeMillis();

  private AtomicInteger consecutive = new AtomicInteger(0);
  private void handleControlMessage(final ControlMessage.Message message) {
    switch (message.getType()) {
      case TaskOutputStart: {
        final ControlMessage.StopTaskDoneMessage m = message.getStopTaskDoneMsg();
        final String reroutingTask = m.getTaskId();
        // find pair task
        final String pairTask = pairStageTaskManager.getPairTaskEdgeId(reroutingTask).left();
        LOG.info("Send task output start to {}", pairTask);
        final ExecutorRepresenter executorRepresenter = executorRegistry
          .getExecutorRepresentor(taskScheduledMap.getTaskExecutorIdMap().get(pairTask));

        executorRepresenter.sendControlMessage(ControlMessage.Message.newBuilder()
          .setId(RuntimeIdManager.generateMessageId())
          .setListenerId(EXECUTOR_MESSAGE_LISTENER_ID.ordinal())
          .setType(ControlMessage.MessageType.TaskOutputStart)
          .setStopTaskMsg(ControlMessage.StopTaskMessage.newBuilder()
            .setTaskId(pairTask)
            .build())
          .build());

        break;
      }
      case GetStateSignal: {
        final ControlMessage.StopTaskDoneMessage m = message.getStopTaskDoneMsg();
        final String reroutingTask = m.getTaskId();
        // find pair task
        final String pairTask = pairStageTaskManager.getPairTaskEdgeId(reroutingTask).left();
        LOG.info("Send GetStateSignal for rerouting data from {} to {}", reroutingTask, pairTask);
        final ExecutorRepresenter executorRepresenter = executorRegistry
          .getExecutorRepresentor(taskScheduledMap.getTaskExecutorIdMap().get(pairTask));

        executorRepresenter.sendControlMessage(ControlMessage.Message.newBuilder()
          .setId(RuntimeIdManager.generateMessageId())
          .setListenerId(EXECUTOR_MESSAGE_LISTENER_ID.ordinal())
          .setType(ControlMessage.MessageType.GetStateSignal)
          .setStopTaskMsg(ControlMessage.StopTaskMessage.newBuilder()
            .setTaskId(pairTask)
            .build())
          .build());

        break;
      }
      case R3PairInputOutputStart: {
        final ControlMessage.StopTaskDoneMessage m = message.getStopTaskDoneMsg();
        final String reroutingTask = m.getTaskId();
        // find pair task
        final String pairTask = pairStageTaskManager.getPairTaskEdgeId(reroutingTask).left();
        LOG.info("Send R3PairInputOutputStart for rerouting data from {} to {}", reroutingTask, pairTask);
        final ExecutorRepresenter executorRepresenter = executorRegistry
          .getExecutorRepresentor(taskScheduledMap.getTaskExecutorIdMap().get(pairTask));

        executorRepresenter.sendControlMessage(ControlMessage.Message.newBuilder()
          .setId(RuntimeIdManager.generateMessageId())
          .setListenerId(EXECUTOR_MESSAGE_LISTENER_ID.ordinal())
          .setType(ControlMessage.MessageType.R3PairInputOutputStart)
          .setStopTaskMsg(ControlMessage.StopTaskMessage.newBuilder()
            .setTaskId(pairTask)
            .build())
          .build());
        break;
      }
      case InputStart: {
        final ControlMessage.StopTaskDoneMessage m = message.getStopTaskDoneMsg();
        final String reroutingTask = m.getTaskId();
        // find pair task
        final String pairTask = pairStageTaskManager.getPairTaskEdgeId(reroutingTask).left();
        LOG.info("Send GetStateSignal for rerouting data from {} to {}", reroutingTask, pairTask);
        final ExecutorRepresenter executorRepresenter = executorRegistry
          .getExecutorRepresentor(taskScheduledMap.getTaskExecutorIdMap().get(pairTask));

        executorRepresenter.sendControlMessage(ControlMessage.Message.newBuilder()
          .setId(RuntimeIdManager.generateMessageId())
          .setListenerId(EXECUTOR_MESSAGE_LISTENER_ID.ordinal())
          .setType(ControlMessage.MessageType.InputStart)
          .setStopTaskMsg(ControlMessage.StopTaskMessage.newBuilder()
            .setTaskId(pairTask)
            .build())
          .build());

        break;
      }
      case LatencyCollection: {
        final long curr = System.currentTimeMillis();
        final ControlMessage.LatencyCollectionMessage msg = message.getLatencyMsg();
        LOG.info("Latency in Master in {}: {}", msg.getExecutorId(), msg.getLatency());
        if (curr - st >= 180000 && msg.getLatency() >= evalConf.latencyLimit) {
          LOG.info("Request to kill this test.. latency {}", msg.getLatency());
          requestContainerThread.execute(() -> {
            stopLambdaContainer(10);
            try {
              Thread.sleep(300);
            } catch (InterruptedException e) {
              e.printStackTrace();
            }
            clientRPC.send(ControlMessage.DriverToClientMessage.newBuilder()
              .setType(ControlMessage.DriverToClientMessageType.KillAll).build());
          });
        } else {
          consecutive.set(0);
        }
        break;
      }
      case StopTaskDone: {
        requestContainerThread.execute(() -> {
          try {
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
          } catch (final Exception e) {
            e.printStackTrace();
            throw new RuntimeException(e);
          }
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
