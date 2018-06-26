/*
 * Copyright (C) 2018 Seoul National University
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
package edu.snu.nemo.runtime.master.scheduler;

import edu.snu.nemo.runtime.common.plan.Task;
import edu.snu.nemo.runtime.master.resource.ExecutorRepresenter;

import javax.inject.Inject;
import java.util.Arrays;
import java.util.List;

/**
 * Temporary class to implement stacked scheduling policy.
 * At now, policies are injected through Tang, but have to be configurable by users
 * when Nemo supports job-wide execution property.
 * TODO #69: Support job-wide execution property.
 */
public final class CompositeSchedulingPredicate implements SchedulingPredicate {
  private final List<SchedulingPredicate> schedulingPolicies;

  @Inject
  private CompositeSchedulingPredicate(
      final SourceLocationAwareSchedulingPredicate sourceLocationAwareSchedulingPredicate,
      final MinOccupancyFirstSchedulingPredicate minOccupancyFirstSchedulingPredicate,
      final FreeSlotSchedulingPredicate freeSlotSchedulingPredicate,
      final ContainerTypeAwareSchedulingPredicate containerTypeAwareSchedulingPredicate) {
    schedulingPolicies = Arrays.asList(
        freeSlotSchedulingPredicate,
        containerTypeAwareSchedulingPredicate,
        sourceLocationAwareSchedulingPredicate,
        minOccupancyFirstSchedulingPredicate);
  }

  @Override
  public boolean testSchedulability(final ExecutorRepresenter executor, final Task task) {
    for (final SchedulingPredicate schedulingPredicate : schedulingPolicies) {
      if (!schedulingPredicate.testSchedulability(executor, task)) {
        return false;
      }
    }
    return true;
  }
}
