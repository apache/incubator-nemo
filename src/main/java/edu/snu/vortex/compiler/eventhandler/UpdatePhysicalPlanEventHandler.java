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
package edu.snu.vortex.compiler.eventhandler;

import edu.snu.vortex.common.PubSubEventHandlerWrapper;
import edu.snu.vortex.common.Pair;
import edu.snu.vortex.runtime.common.plan.physical.PhysicalPlan;
import edu.snu.vortex.runtime.common.plan.physical.TaskGroup;
import edu.snu.vortex.runtime.master.scheduler.Scheduler;

import javax.inject.Inject;

/**
 * Class for handling event to update physical plan to the scheduler.
 */
public final class UpdatePhysicalPlanEventHandler implements CompilerEventHandler<UpdatePhysicalPlanEvent> {
  @Inject
  private UpdatePhysicalPlanEventHandler(final PubSubEventHandlerWrapper pubSubEventHandlerWrapper) {
    // You can see the list of events that are handled by this handler.
    pubSubEventHandlerWrapper.getPubSubEventHandler().subscribe(UpdatePhysicalPlanEvent.class, this);
  }

  @Override
  public void onNext(final UpdatePhysicalPlanEvent updatePhysicalPlanEvent) {
    final Scheduler scheduler = updatePhysicalPlanEvent.getScheduler();
    final PhysicalPlan newPlan = updatePhysicalPlanEvent.getNewPhysicalPlan();
    final Pair<String, TaskGroup> taskInfo = updatePhysicalPlanEvent.getTaskInfo();

    scheduler.updateJob(newPlan, taskInfo);
  }
}
