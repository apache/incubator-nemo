/*
 * Copyright (C) 2017 Seoul National University
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package edu.snu.vortex.runtime.master.scheduler;

import edu.snu.vortex.runtime.common.comm.ExecutorMessage;
import edu.snu.vortex.runtime.common.plan.physical.PhysicalPlan;
import edu.snu.vortex.runtime.master.ExecutionStateManager;
import edu.snu.vortex.runtime.master.ExecutorRepresenter;
import org.apache.reef.tang.annotations.DefaultImplementation;

/**
 * Defines the policy by which {@link BatchScheduler} assigns task groups to executors.
 */
@DefaultImplementation(BatchScheduler.class)
public interface Scheduler {
  ExecutionStateManager scheduleJob(final PhysicalPlan physicalPlan);

  void onExecutorAdded(final ExecutorRepresenter executor);

  void onExecutorRemoved(final ExecutorRepresenter executor);

  void onTaskGroupStateChanged(final String executorId,
                               final ExecutorMessage.TaskGroupStateChangedMsg message);

  void terminate();
}
