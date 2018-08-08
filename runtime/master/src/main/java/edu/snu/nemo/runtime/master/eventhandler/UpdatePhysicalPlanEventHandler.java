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
package edu.snu.nemo.runtime.master.eventhandler;

import edu.snu.nemo.common.eventhandler.CompilerEventHandler;
import edu.snu.nemo.runtime.common.eventhandler.UpdatePhysicalPlanEvent;
import edu.snu.nemo.runtime.common.plan.PhysicalPlan;
import edu.snu.nemo.runtime.master.scheduler.Scheduler;

import javax.inject.Inject;

/**
 * Class for handling event to update physical plan to the scheduler.
 */
public final class UpdatePhysicalPlanEventHandler implements CompilerEventHandler<UpdatePhysicalPlanEvent> {
  private Scheduler scheduler;

  @Inject
  private UpdatePhysicalPlanEventHandler() {
  }

  public void setScheduler(final Scheduler scheduler) {
    this.scheduler = scheduler;
  }

  @Override
  public Class<UpdatePhysicalPlanEvent> getEventClass() {
    return UpdatePhysicalPlanEvent.class;
  }

  @Override
  public void onNext(final UpdatePhysicalPlanEvent updatePhysicalPlanEvent) {
    final PhysicalPlan newPlan = updatePhysicalPlanEvent.getNewPhysicalPlan();

    this.scheduler.updateJob(newPlan.getId(), newPlan);
  }
}
