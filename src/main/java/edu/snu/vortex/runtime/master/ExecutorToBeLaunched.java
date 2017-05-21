package edu.snu.vortex.runtime.master;

import edu.snu.vortex.runtime.common.RuntimeAttribute;

/**
 * Hack for.
 * TODO #60: Specify Types in Requesting Containers
 */
final class ExecutorToBeLaunched {
  private final RuntimeAttribute resourceType;
  private final int executorCapacity;

  ExecutorToBeLaunched(final RuntimeAttribute resourceType, final int executorCapacity) {
    this.resourceType = resourceType;
    this.executorCapacity = executorCapacity;
  }

  RuntimeAttribute getResourceType() {
    return resourceType;
  }

  int getExecutorCapacity() {
    return executorCapacity;
  }
}


