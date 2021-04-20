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
package org.apache.nemo.driver;

import org.apache.nemo.common.Pair;
import org.apache.nemo.conf.EvalConf;
import org.apache.nemo.offloading.client.*;
import org.apache.nemo.offloading.common.ServerlessExecutorProvider;
import org.apache.nemo.common.ir.IdManager;
import org.apache.nemo.compiler.optimizer.pass.compiletime.annotating.ResourceSitePass;
import org.apache.nemo.conf.JobConf;
import org.apache.nemo.common.RuntimeIdManager;
import org.apache.nemo.runtime.common.NettyVMStateStore;
import org.apache.nemo.runtime.common.comm.ControlMessage;
import org.apache.nemo.runtime.executor.*;
import org.apache.nemo.runtime.executor.bytetransfer.DefaultByteTransportImpl;
import org.apache.nemo.runtime.executor.common.controlmessages.DefaultControlEventHandlerImpl;
import org.apache.nemo.runtime.executor.common.monitoring.CpuBottleneckDetectorImpl;
import org.apache.nemo.runtime.executor.offloading.*;
import org.apache.nemo.runtime.executor.common.*;
import org.apache.nemo.runtime.executor.common.datatransfer.InputPipeRegister;
import org.apache.nemo.runtime.executor.common.datatransfer.IntermediateDataIOFactory;
import org.apache.nemo.runtime.executor.common.datatransfer.PipeManagerWorker;
import org.apache.nemo.offloading.common.StateStore;
import org.apache.nemo.runtime.executor.common.PipeManagerWorkerImpl;
import org.apache.nemo.runtime.executor.common.DefaltIntermediateDataIOFactoryImpl;
import org.apache.nemo.runtime.executor.common.datatransfer.DefaultOutputCollectorGeneratorImpl;
import org.apache.nemo.runtime.master.ClientRPC;
import org.apache.nemo.runtime.master.BroadcastManagerMaster;
import org.apache.nemo.runtime.master.JobScaler;
import org.apache.nemo.runtime.master.RuntimeMaster;
import org.apache.commons.lang3.SerializationUtils;
import org.apache.commons.lang3.concurrent.BasicThreadFactory;
import org.apache.nemo.runtime.executor.common.ByteTransport;
import org.apache.nemo.runtime.message.MessageParameters;
import org.apache.nemo.runtime.message.NemoNameServer;
import org.apache.reef.annotations.audience.DriverSide;
import org.apache.reef.driver.client.JobMessageObserver;
import org.apache.reef.driver.context.ActiveContext;
import org.apache.reef.driver.context.ContextConfiguration;
import org.apache.reef.driver.context.FailedContext;
import org.apache.reef.driver.evaluator.AllocatedEvaluator;
import org.apache.reef.driver.evaluator.FailedEvaluator;
import org.apache.reef.driver.evaluator.JVMProcessFactory;
import org.apache.reef.io.network.util.StringIdentifierFactory;
import org.apache.reef.tang.Configuration;
import org.apache.reef.tang.Configurations;
import org.apache.reef.tang.Tang;
import org.apache.reef.tang.annotations.Parameter;
import org.apache.reef.tang.annotations.Unit;
import org.apache.reef.wake.EventHandler;
import org.apache.reef.wake.IdentifierFactory;
import org.apache.reef.wake.remote.address.LocalAddressProvider;
import org.apache.reef.wake.time.event.StartTime;
import org.apache.reef.wake.time.event.StopTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import java.io.Serializable;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.logging.LogManager;
import java.util.stream.Collectors;

/**
 * REEF Driver for Nemo.
 */
@Unit
@DriverSide
public final class NemoDriver {
  private static final Logger LOG = LoggerFactory.getLogger(NemoDriver.class.getName());

  private final NemoNameServer nameServer;
  private final LocalAddressProvider localAddressProvider;

  private final String resourceSpecificationString;

  private final UserApplicationRunner userApplicationRunner;
  private final RuntimeMaster runtimeMaster;
  private final String jobId;
  private final String localDirectory;
  private final String glusterDirectory;
  private final ClientRPC clientRPC;

  private static ExecutorService runnerThread = Executors.newSingleThreadExecutor(
     new BasicThreadFactory.Builder().namingPattern("User App thread-%d").build());

  // Client for sending log messages
  private final RemoteClientMessageLoggingHandler handler;

  private final JVMProcessFactory jvmProcessFactory;

  private final EvalConf evalConf;
  private final JobScaler jobScaler;

  private final ExecutorService singleThread = Executors.newSingleThreadExecutor();

  @Inject
  private NemoDriver(final UserApplicationRunner userApplicationRunner,
                     final RuntimeMaster runtimeMaster,
                     final NemoNameServer nameServer,
                     final LocalAddressProvider localAddressProvider,
                     final JobMessageObserver client,
                     final ClientRPC clientRPC,
                     final EvalConf evalConf,
                     @Parameter(JobConf.ExecutorJSONContents.class) final String resourceSpecificationString,
                     @Parameter(JobConf.BandwidthJSONContents.class) final String bandwidthString,
                     @Parameter(JobConf.JobId.class) final String jobId,
                     @Parameter(JobConf.FileDirectory.class) final String localDirectory,
                     @Parameter(JobConf.GlusterVolumeDirectory.class) final String glusterDirectory,
                     final JVMProcessFactory jvmProcessFactory,
                     final JobScaler jobScaler) {
    IdManager.setInDriver();
    this.userApplicationRunner = userApplicationRunner;
    this.runtimeMaster = runtimeMaster;
    this.nameServer = nameServer;
    this.evalConf = evalConf;
    this.localAddressProvider = localAddressProvider;
    this.resourceSpecificationString = resourceSpecificationString;
    this.jobId = jobId;
    this.jobScaler = jobScaler;
    this.localDirectory = localDirectory;
    this.glusterDirectory = glusterDirectory;
    this.handler = new RemoteClientMessageLoggingHandler(client);
    this.jvmProcessFactory = jvmProcessFactory;
    this.clientRPC = clientRPC;
    // TODO #69: Support job-wide execution property
    ResourceSitePass.setBandwidthSpecificationString(bandwidthString);

    clientRPC.registerHandler(ControlMessage.ClientToDriverMessageType.Scaling, message -> {
      singleThread.execute(() -> {
        final String decision = message.getScalingMsg().getDecision();

        if (evalConf.enableOffloading) {
          synchronized (this) {
            LOG.info("Receive scaling decision {}", message.getScalingMsg().getInfo());

            if (decision.equals("o") || decision.equals("no") || decision.equals("oratio") || decision.equals("op")) {
              // Op: priority prepareOffloading
              jobScaler.scalingOut(message.getScalingMsg());
            } else if (decision.equals("i")) {
              jobScaler.scalingIn();
            } else if (decision.equals("pa")) {
              jobScaler.proactive(message.getScalingMsg());
            } else if (decision.equals("info")) {
              jobScaler.broadcastInfo(message.getScalingMsg());

            } else if (decision.equals("add-yarn")) {
              final String[] args = message.getScalingMsg().getInfo().split(" ");
              final int num = new Integer(args[1]);
              runtimeMaster.requestContainer(resourceSpecificationString,
                false, false, "Evaluator", num);
            } else if (decision.equals("add-lambda-executor")) {
              // scaling executor for Lambda
              final String[] args = message.getScalingMsg().getInfo().split(" ");
              final int num = new Integer(args[1]);
              final int capacity = new Integer(args[2]);
              final int slot = new Integer(args[3]);
              final int memory = new Integer(args[4]);
              runtimeMaster.requestLambdaContainer(num, capacity, slot, memory);
            } else if (decision.equals("stop-lambda-executor")) {
              final String[] args = message.getScalingMsg().getInfo().split(" ");
              final int num = new Integer(args[1]);
              runtimeMaster.stopLambdaContainer(num);
            } else if (decision.equals("add-offloading-executor")) {
              final String[] args = message.getScalingMsg().getInfo().split(" ");
              final int num = new Integer(args[1]);
              runtimeMaster.stopLambdaContainer(num);
            } else if (decision.equals("activate-lambda")) {
              final String[] args = message.getScalingMsg().getInfo().split(" ");
              runtimeMaster.activateLambda();
            } else if (decision.equals("deactivate-lambda")) {
              final String[] args = message.getScalingMsg().getInfo().split(" ");
              runtimeMaster.deactivateLambda();
            } else if (decision.equals("send-bursty")) {
              final String[] args = message.getScalingMsg().getInfo().split(" ");
              final int num = new Integer(args[1]);
              runtimeMaster.sendBursty(num);
            } else if (decision.equals("finish-bursty")) {
              final String[] args = message.getScalingMsg().getInfo().split(" ");
              final int num = new Integer(args[1]);
              runtimeMaster.finishBursty(num);
            } else if (decision.equals("invoke-partial-offloading")) {
              final String[] args = message.getScalingMsg().getInfo().split(" ");
              runtimeMaster.invokePartialOffloading();
            } else if (decision.equals("offload-task")) {
              final String[] args = message.getScalingMsg().getInfo().split(" ");
              final int num = new Integer(args[1]);
              final int stageId = new Integer(args[2]);
              runtimeMaster.offloadTask(num, stageId);

            } else if (decision.equals("deoffload-task")) {
              final String[] args = message.getScalingMsg().getInfo().split(" ");
              final int num = new Integer(args[1]);
              final int stageId = new Integer(args[2]);
              runtimeMaster.deoffloadTask(num, stageId);

            } else if (decision.equals("conditional-routing")) {
              final String[] args = message.getScalingMsg().getInfo().split(" ");
              final boolean partial = new Boolean(args[1]);
              final double percent = new Double(args[2]);
              runtimeMaster.triggerConditionalRouting(partial, percent);
            } else if (decision.equals("redirection")) {
              // FOR CR ROUTING!!
              // VM -> Lambda
              final String[] args = message.getScalingMsg().getInfo().split(" ");
              final int num = new Integer(args[1]);
              final String[] stageIds = args[2].split(",");
              final List<String> stages =
                Arrays.asList(stageIds).stream().map(sid -> "Stage" + sid)
                  .collect(Collectors.toList());
              runtimeMaster.redirectionToLambda(num, stages);
            }  else if (decision.equals("redirection-done")) {
              // FOR CR ROUTING!!
              // Lambda -> VM
              final String[] args = message.getScalingMsg().getInfo().split(" ");
              final int num = new Integer(args[1]);
              final String[] stageIds = args[2].split(",");
              final List<String> stages =
                Arrays.asList(stageIds).stream().map(sid -> "Stage" + sid)
                  .collect(Collectors.toList());
              runtimeMaster.redirectionDoneToLambda(num, stages);
            } else if (decision.equals("move-task-lambda")) {
              final String[] args = message.getScalingMsg().getInfo().split(" ");
              final int num = new Integer(args[1]);
              final String[] stageIds = args[2].split(",");
              final List<String> stages =
                Arrays.asList(stageIds).stream().map(sid -> "Stage" + sid)
                  .collect(Collectors.toList());
              for (int i = stages.size() - 1; i >= 0; i--) {
                jobScaler.sendTaskStopSignal(num, true, Collections.singletonList(stages.get(i)));
                try {
                  Thread.sleep(150);
                } catch (InterruptedException e) {
                  e.printStackTrace();
                }
              }
            } else if (decision.equals("move-task")) {
              final String[] args = message.getScalingMsg().getInfo().split(" ");
              final int num = new Integer(args[1]);
              final String[] stageIds = args[2].split(",");
              final List<String> stages =
                Arrays.asList(stageIds).stream().map(sid -> "Stage" + sid)
                  .collect(Collectors.toList());
              for (int i = stages.size() - 1; i >= 0; i--) {
                jobScaler.sendTaskStopSignal(num, false, Collections.singletonList(stages.get(i)));
                try {
                  Thread.sleep(150);
                } catch (InterruptedException e) {
                  e.printStackTrace();
                }
              }
              // runtimeMaster.triggerConditionalRouting(true, evalConf.partialPercent * 0.01);
            } else if (decision.equals("reclaim-task")) {
              final String[] args = message.getScalingMsg().getInfo().split(" ");
              final int num = new Integer(args[1]);
              final String[] stageIds = args[2].split(",");
              final List<String> stages =
                Arrays.asList(stageIds).stream().map(sid -> "Stage" + sid)
                  .collect(Collectors.toList());
              // runtimeMaster.triggerConditionalRouting(false, 0);
              jobScaler.sendPrevMovedTaskStopSignal(num, stages);
            } else if (decision.equals("throttle-source")) {
              final String[] args = message.getScalingMsg().getInfo().split(" ");
              final int num = new Integer(args[1]);
              runtimeMaster.throttleSource(num);
            } else {
              throw new RuntimeException("Invalid scaling decision " + decision);
            }
          }
        }

      });
    });

    clientRPC.registerHandler(ControlMessage.ClientToDriverMessageType.LaunchDAG, message -> {
      startSchedulingUserDAG(message.getLaunchDAG().getDag());
      final Map<Serializable, Object> broadcastVars =
        SerializationUtils.deserialize(message.getLaunchDAG().getBroadcastVars().toByteArray());
      BroadcastManagerMaster.registerBroadcastVariablesFromClient(broadcastVars);
    });
    clientRPC.registerHandler(ControlMessage.ClientToDriverMessageType.DriverShutdown, message -> shutdown());
    // Send DriverStarted message to the client
    clientRPC.send(ControlMessage.DriverToClientMessage.newBuilder()
        .setType(ControlMessage.DriverToClientMessageType.DriverStarted).build());
  }

  /**
   * Setup the logger that forwards logging messages to the client.
   */
  private void setUpLogger() {
    final java.util.logging.Logger rootLogger = LogManager.getLogManager().getLogger("");
    rootLogger.addHandler(handler);
  }

  /**
   * Trigger shutdown of the driver and the runtime master.
   */
  private void shutdown() {
    LOG.info("Driver shutdown initiated");
    // runnerThread.execute(runtimeMaster::terminate);
    runnerThread.shutdownNow();
    runtimeMaster.terminate();
    clientRPC.send(ControlMessage.DriverToClientMessage.newBuilder()
      .setType(ControlMessage.DriverToClientMessageType.DriverShutdowned).build());
  }

  /**
   * Driver started.
   */
  public final class StartHandler implements EventHandler<StartTime> {
    @Override
    public void onNext(final StartTime startTime) {
      setUpLogger();
      if (evalConf.optimizationPolicy.contains("R2") || evalConf.optimizationPolicy.contains("R3")) {
        runtimeMaster.requestContainer(
          resourceSpecificationString, true, false, "Evaluator", 0);
      } else {
        runtimeMaster.requestContainer(
          resourceSpecificationString, false, false, "Evaluator", 0);
      }
    }
  }

  /**
   * Container allocated.
   */
  public final class AllocatedEvaluatorHandler implements EventHandler<AllocatedEvaluator> {
    @Override
    public void onNext(final AllocatedEvaluator allocatedEvaluator) {
      LOG.info("runtime name: " + allocatedEvaluator.getEvaluatorDescriptor().getRuntimeName());

      if (!runtimeMaster.isOffloadingExecutorEvaluator()) {
        final String executorId = RuntimeIdManager.generateExecutorId();
        runtimeMaster.onContainerAllocated(executorId, allocatedEvaluator,
          getExecutorConfiguration(executorId));
        //final JVMProcess jvmProcess = jvmProcessFactory.newEvaluatorProcess()
        //  .addOption("-Dio.netty.leakDetection.level=advanced");
        //allocatedEvaluator.setProcess(jvmProcess);
      } else {
        final Pair<String, Integer> nameAndPort = runtimeMaster.getOffloadingExecutorPort(
          allocatedEvaluator.getEvaluatorDescriptor()
              .getNodeDescriptor().getInetSocketAddress().getHostName());

        runtimeMaster.onContainerAllocated(nameAndPort.left(), allocatedEvaluator,
          getOffloadingConfiguration(nameAndPort.left(), nameAndPort.right()));
      }
    }
  }

  private Configuration getOffloadingConfiguration(final String executorId,
                                                   final int port) {
    final Configuration contextConfiguration = ContextConfiguration.CONF
      .set(ContextConfiguration.IDENTIFIER, executorId) // We set: contextId = executorId
      .set(ContextConfiguration.ON_CONTEXT_STARTED, OffloadingContext.ContextStartHandler.class)
      .set(ContextConfiguration.ON_CONTEXT_STOP, OffloadingContext.ContextStopHandler.class)
      .build();

    final Configuration conf = Tang.Factory.getTang().newConfigurationBuilder()
      .bindNamedParameter(VMWorkerExecutor.VMWorkerPort.class,
        Integer.toString(port))
      .bindNamedParameter(VMWorkerExecutor.HandlerTimeout.class, Integer.toString(evalConf.handlerTimeout))
      .build();

    return Configurations.merge(contextConfiguration, conf);
  }

  /**
   * Context active.
   */
  public final class ActiveContextHandler implements EventHandler<ActiveContext> {
    @Override
    public void onNext(final ActiveContext activeContext) {
      final boolean finalExecutorLaunched = runtimeMaster.onExecutorLaunched(activeContext);

      if (finalExecutorLaunched) {
        clientRPC.send(ControlMessage.DriverToClientMessage.newBuilder()
            .setType(ControlMessage.DriverToClientMessageType.DriverReady).build());
      }
    }
  }

  /**
   * Start to schedule a submitted user DAG.
   *
   * @param dagString  the serialized DAG to schedule.
   */
  private void startSchedulingUserDAG(final String dagString) {
    runnerThread.execute(() -> {
      userApplicationRunner.run(dagString);
      // send driver notification that user application is done.
      clientRPC.send(ControlMessage.DriverToClientMessage.newBuilder()
          .setType(ControlMessage.DriverToClientMessageType.ExecutionDone).build());
      // flush metrics
      runtimeMaster.flushMetrics();
    });
  }

  /**
   * Evaluator failed.
   */
  public final class FailedEvaluatorHandler implements EventHandler<FailedEvaluator> {
    @Override
    public void onNext(final FailedEvaluator failedEvaluator) {
      runtimeMaster.onExecutorFailed(failedEvaluator);
      shutdown();
    }
  }

  /**
   * Context failed.
   */
  public final class FailedContextHandler implements EventHandler<FailedContext> {
    @Override
    public void onNext(final FailedContext failedContext) {
      shutdown();
      throw new RuntimeException(failedContext.getId() + " failed. See driver's log for the stack trace in executor.",
          failedContext.asError());
    }
  }

  /**
   * Driver stopped.
   */
  public final class DriverStopHandler implements EventHandler<StopTime> {
    @Override
    public void onNext(final StopTime stopTime) {
      handler.close();
      clientRPC.shutdown();
    }
  }

  private Class<? extends OffloadingManager> getOffloadingManager() {
    if (evalConf.offloadingManagerType.equals("shared")) {
      return SingleWorkerOffloadingManagerImpl.class;
    } else  if (evalConf.offloadingManagerType.equals("multiple")) {
      return SingleTaskMultipleWorkersOffloadingManagerImpl.class;
    }  else  if (evalConf.offloadingManagerType.equals("multiple-merge")) {
      return MultipleWorkersMergingOffloadingManagerImpl.class;
    } else {
      throw new RuntimeException("invalid offloading manager type " + evalConf.offloadingManagerType);
    }
  }

  private Configuration getExecutorConfiguration(final String executorId) {
    final Configuration executorConfiguration = JobConf.EXECUTOR_CONF
        .set(JobConf.EXECUTOR_ID, executorId)
        .set(JobConf.GLUSTER_DISK_DIRECTORY, glusterDirectory)
        .set(JobConf.LOCAL_DISK_DIRECTORY, localDirectory)
        .set(JobConf.JOB_ID, jobId)
        .build();

    final Configuration contextConfiguration = ContextConfiguration.CONF
        .set(ContextConfiguration.IDENTIFIER, executorId) // We set: contextId = executorId
        .set(ContextConfiguration.ON_CONTEXT_STARTED, NemoContext.ContextStartHandler.class)
        .set(ContextConfiguration.ON_CONTEXT_STOP, NemoContext.ContextStopHandler.class)
        .build();

    final Configuration ncsConfiguration =  getExecutorNcsConfiguration();
    final Configuration messageConfiguration = getExecutorMessageConfiguration(executorId);
    final Configuration evalConfiguration = evalConf.getConfiguration();

    final Configuration c = Tang.Factory.getTang().newConfigurationBuilder()
      .bindImplementation(PipeManagerWorker.class, PipeManagerWorkerImpl.class)
      .bindImplementation(InputPipeRegister.class, PipeManagerWorkerImpl.class)
      .bindImplementation(StateStore.class, NettyVMStateStore.class)
      // .bindImplementation(OffloadingManager.class, getOffloadingManager())
      .bindImplementation(ControlEventHandler.class, DefaultControlEventHandlerImpl.class)
      .bindImplementation(SerializerManager.class, DefaultSerializerManagerImpl.class)
      .bindImplementation(IntermediateDataIOFactory.class, DefaltIntermediateDataIOFactoryImpl.class)
      // .bindImplementation(OffloadingWorkerFactory.class, DefaultOffloadingWorkerFactory.class)
      .bindImplementation(OutputCollectorGenerator.class, DefaultOutputCollectorGeneratorImpl.class)
      .bindImplementation(MetricMessageSender.class, MetricManagerWorker.class)
      .bindImplementation(ByteTransport.class, DefaultByteTransportImpl.class)
      .bindNamedParameter(EvalConf.ExecutorOnLambda.class, Boolean.toString(false))
      .bindImplementation(CpuBottleneckDetector.class, CpuBottleneckDetectorImpl.class)
      .build();

    return Configurations.merge(c,
      executorConfiguration,
      contextConfiguration,
      ncsConfiguration,
      messageConfiguration,
      evalConfiguration);
  }


  private Configuration getExecutorNcsConfiguration() {
    return Tang.Factory.getTang().newConfigurationBuilder()
      .bindNamedParameter(MessageParameters.NameServerPort.class, Integer.toString(nameServer.getPort()))
      .bindNamedParameter(MessageParameters.NameServerAddr.class, localAddressProvider.getLocalAddress())
      .bindImplementation(IdentifierFactory.class, StringIdentifierFactory.class)
      .bindImplementation(ServerlessExecutorProvider.class, ServerlessExecutorProviderImpl.class) // TODO: fix
        .build();
  }

  private Configuration getExecutorMessageConfiguration(final String executorId) {
    return Tang.Factory.getTang().newConfigurationBuilder()
        .bindNamedParameter(MessageParameters.SenderId.class, executorId)
        .build();
  }
}
