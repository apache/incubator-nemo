package org.apache.nemo.runtime.master.resource;

import org.apache.nemo.runtime.common.RuntimeIdManager;

/**
 * Represents the specifications of a resource.
 */
// We should clean up and add execution properties related to resource specification.
public final class ResourceSpecification {
  private final String resourceSpecId;
  private final String containerType;
  private final int capacity;
  private final int memory;
  private final int poisonSec; // -1 if this resources is not poisoned

  public ResourceSpecification(final String containerType,
                               final int capacity,
                               final int memory) {
    this(containerType, capacity, memory, -1);
  }

  public ResourceSpecification(final String containerType,
                               final int capacity,
                               final int memory,
                               final int poisonSec) {
    this.resourceSpecId = RuntimeIdManager.generateResourceSpecId();
    this.containerType = containerType;
    this.capacity = capacity;
    this.memory = memory;
    this.poisonSec = poisonSec;
  }

  /**
   * @return The type of the container.
   */
  public String getContainerType() {
    return containerType;
  }

  /**
   * @return The number of Tasks that can be run in this container.
   */
  public int getCapacity() {
    return capacity;
  }

  /**
   * @return Allocated memory for the container, in megabytes.
   */
  public int getMemory() {
    return memory;
  }

  public String getResourceSpecId() {
    return resourceSpecId;
  }

  /**
   * @return -1   if this resource is not poisoned. (for all other normal cases)
   *         >= 0 the expected time to failure by poison. (for fault-handling tests)
   */
  public int getPoisonSec() {
    return poisonSec;
  }
}
