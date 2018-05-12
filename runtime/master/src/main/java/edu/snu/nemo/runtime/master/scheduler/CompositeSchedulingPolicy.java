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

import edu.snu.nemo.runtime.common.plan.physical.ScheduledTaskGroup;
import edu.snu.nemo.runtime.master.resource.ExecutorRepresenter;

import javax.inject.Inject;
import java.util.Arrays;
import java.util.List;

/**
 * Temporary class to implement stacked scheduling policy.
 */
public final class CompositeSchedulingPolicy implements SchedulingPolicy {
  private final List<SchedulingPolicy> schedulingPolicies;

  @Inject
  private CompositeSchedulingPolicy(final SourceLocationAwareSchedulingPolicy sourceLocationAwareSchedulingPolicy,
                                    final RoundRobinSchedulingPolicy roundRobinSchedulingPolicy,
                                    final FreeSlotSchedulingPolicy freeSlotSchedulingPolicy,
                                    final ContainerTypeAwareSchedulingPolicy containerTypeAwareSchedulingPolicy) {
    schedulingPolicies = Arrays.asList(
        freeSlotSchedulingPolicy,
        containerTypeAwareSchedulingPolicy,
        sourceLocationAwareSchedulingPolicy,
        roundRobinSchedulingPolicy);
  }

  @Override
  public List<ExecutorRepresenter> filterExecutorRepresenters(final List<ExecutorRepresenter> executorRepresenterList,
                                                              final ScheduledTaskGroup scheduledTaskGroup) {
    List<ExecutorRepresenter> candidates = executorRepresenterList;
    for (final SchedulingPolicy schedulingPolicy : schedulingPolicies) {
      candidates = schedulingPolicy.filterExecutorRepresenters(candidates, scheduledTaskGroup);
    }
    return candidates;
  }
}
