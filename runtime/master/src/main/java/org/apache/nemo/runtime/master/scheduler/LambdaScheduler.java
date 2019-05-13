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

import com.amazonaws.ClientConfiguration;
import com.amazonaws.services.lambda.AWSLambda;
import com.amazonaws.services.lambda.AWSLambdaAsync;
import com.amazonaws.services.lambda.AWSLambdaAsyncClientBuilder;
import com.amazonaws.services.lambda.AWSLambdaClientBuilder;
import com.amazonaws.services.lambda.model.InvokeRequest;
import com.amazonaws.services.lambda.model.InvokeResult;
import io.netty.channel.group.ChannelGroup;
import io.netty.channel.group.DefaultChannelGroup;
import io.netty.util.concurrent.GlobalEventExecutor;
import org.apache.nemo.common.ir.Readable;
import org.apache.nemo.offloading.client.NettyServerSideChannelHandler;
import org.apache.nemo.offloading.client.NettyServerTransport;
import org.apache.nemo.runtime.common.plan.PhysicalPlan;
import org.apache.nemo.runtime.common.plan.Stage;
import org.apache.nemo.runtime.common.plan.StageEdge;
import org.apache.nemo.runtime.common.state.TaskState;
import org.apache.nemo.runtime.master.PipeManagerMaster;
import org.apache.nemo.runtime.master.PlanStateManager;
import org.apache.nemo.runtime.master.resource.ExecutorRepresenter;
import org.apache.reef.annotations.audience.DriverSide;
import org.apache.reef.tang.Injector;
import org.apache.reef.tang.JavaConfigurationBuilder;
import org.apache.reef.tang.Tang;
import org.apache.reef.wake.remote.ports.TcpPortProvider;
import org.apache.reef.wake.remote.ports.parameters.TcpPortRangeBegin;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import javax.annotation.concurrent.NotThreadSafe;
import javax.inject.Inject;
import java.util.List;
import java.util.Map;

/**
 * A simple scheduler for batch job with Nemo Lambda Executor.
 * - Keeps track of new executors
 * - Schedules all tasks in the plan at once.
 * - Crashes the system upon any other events (should be fixed in the future)
 * - Never stops running.
 */
@DriverSide
@NotThreadSafe
public final class LambdaScheduler implements Scheduler {
  private static final Logger LOG = LoggerFactory.getLogger(StreamingScheduler.class.getName());

  private final PlanStateManager planStateManager;

  @Inject
  private LambdaScheduler(final TaskDispatcher taskDispatcher,
                     final PendingTaskCollectionPointer pendingTaskCollectionPointer,
                     final ExecutorRegistry executorRegistry,
                     final PlanStateManager planStateManager,
                     final PipeManagerMaster pipeManagerMaster) {
    this.planStateManager = planStateManager;
    LOG.info("Using Lambda Scheduler");
  }

  @Override
  public void schedulePlan(final PhysicalPlan submittedPhysicalPlan,
                           final int maxScheduleAttempt) {

    if (maxScheduleAttempt > 1) {
      throw new UnsupportedOperationException();
    }

    planStateManager.updatePlan(submittedPhysicalPlan, maxScheduleAttempt);
    planStateManager.storeJSON("submitted");

    // We presume serverless function has been deployed and uploaded

    // Prepare tasks
    final List<Stage> allStages = submittedPhysicalPlan.getStageDAG().getTopologicalSort();
    allStages.stream().forEach(stageToSchedule -> {
      LOG.info("Execute new stage");
      final List<StageEdge> stageIncomingEdges =
        submittedPhysicalPlan.getStageDAG().getIncomingEdgesOf(stageToSchedule.getId());
      final List<StageEdge> stageOutgoingEdges =
        submittedPhysicalPlan.getStageDAG().getOutgoingEdgesOf(stageToSchedule.getId());

      // the list of maps between vertex ID and readables.
      final List<Map<String, Readable>> vertexIdToReadables = stageToSchedule.getVertexIdToReadables();
      final List<String> taskIdsToSchedule = planStateManager.getTaskAttemptsToSchedule(stageToSchedule.getId());


      taskIdsToSchedule.forEach(taskId -> {
        AWSLambda client = AWSLambdaClientBuilder.standard().build();
        final JavaConfigurationBuilder jcb = Tang.Factory.getTang().newConfigurationBuilder();
        jcb.bindNamedParameter(TcpPortRangeBegin.class, "5000");
        LOG.info("Schedule task" + taskId + "Init Conf builder");

        final Injector injector = Tang.Factory.getTang().newInjector(jcb.build());
        try {
          final TcpPortProvider tcpPortProvider = injector.getInstance(TcpPortProvider.class);
          final ChannelGroup serverChannelGroup = new DefaultChannelGroup(GlobalEventExecutor.INSTANCE);
          LOG.info("Get tcp port");

          //final OffloadingEventHandler nemoEventHandler = null;
          // assigning event handler is not assigned so we pass null

          NettyServerTransport nettyServerTransport =  new NettyServerTransport(
            tcpPortProvider, new NettyServerSideChannelHandler(serverChannelGroup, null));;

          LOG.info("Init netty server");
          AWSLambdaAsync awsLambda = AWSLambdaAsyncClientBuilder.standard().withClientConfiguration(
            new ClientConfiguration().withMaxConnections(500)).build();

          LOG.info("Create request");
          InvokeRequest request = new InvokeRequest()
            // hard coded for now, need receiving user info
            .withFunctionName("LambdaExecutor")
            .withInvocationType("Event").withLogType("Tail").withClientContext("Lambda Executor " + taskId)
            .withPayload(String.format("{\"address\":\"%s\", \"port\": %d}",
              nettyServerTransport.getPublicAddress(), nettyServerTransport.getPort()));

          LOG.info("Invoke request");
          awsLambda.invoke(request);
          LOG.info("Request invoked!");

          InvokeResult response = client.invoke(request);
          LOG.info(response.toString());
        } catch (Exception e) {
          LOG.info("Exception: Tcp port not acquired! Stop requesting executor");
        }

      });

    });

  }

  @Override
  public void updatePlan(final PhysicalPlan newPhysicalPlan) {
    // LambdaScheduler Dynamic Optimization
    throw new UnsupportedOperationException();
  }

  @Override
  public void onTaskStateReportFromExecutor(final String executorId,
                                            final String taskId,
                                            final int taskAttemptIndex,
                                            final TaskState.State newState,
                                            @Nullable final String vertexPutOnHold,
                                            final TaskState.RecoverableTaskFailureCause failureCause) {
    // LambdaScheduler
    LOG.info("Not supported: onTaskStateReportFromExecutor");
    throw new UnsupportedOperationException();
  }

  @Override
  public void onSpeculativeExecutionCheck() {
    // TODO #228: StreamingScheduler Speculative Execution
    throw new UnsupportedOperationException();
  }

  @Override
  public void onExecutorAdded(final ExecutorRepresenter executorRepresenter) {
    LOG.info("Not supported: onExecutorAdded");
    throw new UnsupportedOperationException();
  }

  @Override
  public void onExecutorRemoved(final String executorId) {
    // TODO #226: StreamingScheduler Fault Tolerance
    throw new UnsupportedOperationException();
  }

  @Override
  public void terminate() {
    LOG.info("terminate");
  }
}
