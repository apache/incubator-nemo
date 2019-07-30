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

import com.amazonaws.regions.Regions;
import com.amazonaws.services.lambda.AWSLambda;
import com.amazonaws.services.lambda.AWSLambdaClientBuilder;
import com.amazonaws.services.lambda.model.InvokeRequest;
import com.amazonaws.services.lambda.model.InvokeResult;
import org.apache.nemo.common.ir.Readable;
import org.apache.nemo.runtime.common.RuntimeIdManager;
import org.apache.nemo.runtime.common.plan.PhysicalPlan;
import org.apache.nemo.runtime.common.plan.Stage;
import org.apache.nemo.runtime.common.plan.StageEdge;
import org.apache.nemo.runtime.common.state.TaskState;
import org.apache.nemo.runtime.master.PipeManagerMaster;
import org.apache.nemo.runtime.master.PlanStateManager;
import org.apache.nemo.runtime.master.resource.ExecutorRepresenter;
import org.apache.reef.annotations.audience.DriverSide;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import javax.annotation.concurrent.NotThreadSafe;
import javax.inject.Inject;
import java.io.UnsupportedEncodingException;
import java.util.Base64;
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
  private final TaskDispatcher taskDispatcher;
  private final ExecutorRegistry executorRegistry;

  @Inject
  private LambdaScheduler(final TaskDispatcher taskDispatcher,
                     final PendingTaskCollectionPointer pendingTaskCollectionPointer,
                     final ExecutorRegistry executorRegistry,
                     final PlanStateManager planStateManager,
                     final PipeManagerMaster pipeManagerMaster) {
    this.planStateManager = planStateManager;
    this.taskDispatcher = taskDispatcher;
    this.executorRegistry = executorRegistry;

    LOG.info("##### Using Lambda Scheduler #####");
  }

  @Override
  public void schedulePlan(final PhysicalPlan submittedPhysicalPlan,
                           final int maxScheduleAttempt) {

    if (maxScheduleAttempt > 1) {
      throw new UnsupportedOperationException();
    }
    LOG.info("##### Lambda Scheduler schedules plan#####");
    planStateManager.updatePlan(submittedPhysicalPlan, maxScheduleAttempt);
    planStateManager.storeJSON("submitted");

    // We presume serverless function has been deployed and uploaded
    Regions region = Regions.fromName("ap-northeast-1");
    AWSLambda client = AWSLambdaClientBuilder.standard().withRegion(region).build();

    // Prepare tasks
    final List<Stage> allStages = submittedPhysicalPlan.getStageDAG().getTopologicalSort();
    allStages.stream().forEach(stageToSchedule -> {
      LOG.info("##### Execute new stage #####");
      final List<StageEdge> stageIncomingEdges =
        submittedPhysicalPlan.getStageDAG().getIncomingEdgesOf(stageToSchedule.getId());
      final List<StageEdge> stageOutgoingEdges =
        submittedPhysicalPlan.getStageDAG().getOutgoingEdgesOf(stageToSchedule.getId());

      // the list of maps between vertex ID and readables.
      final List<Map<String, Readable>> vertexIdToReadables = stageToSchedule.getVertexIdToReadables();
      final List<String> taskIdsToSchedule = planStateManager.getTaskAttemptsToSchedule(stageToSchedule.getId());

      taskIdsToSchedule.forEach(taskId -> {
        final int index = RuntimeIdManager.getIndexFromTaskId(taskId);
        stageOutgoingEdges.forEach(outEdge -> {
          LOG.info("###### Create request ######");
          try {
            byte [] byteEncoded = outEdge.toString().getBytes();
            String json = "{\"d\":\"" + Base64.getEncoder().encodeToString(byteEncoded) + "\"}";
            InvokeRequest request = new InvokeRequest()
              // hard coded for now, need receiving user info
              .withFunctionName("lambda-dev-executor")
              .withInvocationType("Event").withLogType("Tail").withClientContext("Lambda Executor " + taskId)
              .withPayload(json);
            InvokeResult response = client.invoke(request);
            LOG.info("###### Request invoked! #####");
          } catch (Exception e) {
            e.printStackTrace();
          }
        });
      });
    });
    LOG.info("##### One scheduled plan dispatched! #####");
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
    LOG.info("{} added (node: {})", executorRepresenter.getExecutorId(), executorRepresenter.getNodeName());
    taskDispatcher.onExecutorSlotAvailable();
    executorRegistry.registerExecutor(executorRepresenter);
  }

  @Override
  public void onExecutorRemoved(final String executorId) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void terminate() {
    LOG.info("terminate");
  }
}
