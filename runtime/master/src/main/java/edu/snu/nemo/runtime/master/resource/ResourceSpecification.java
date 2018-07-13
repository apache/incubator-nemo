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
package edu.snu.nemo.runtime.master.resource;

import edu.snu.nemo.runtime.common.RuntimeIdGenerator;

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
    this.resourceSpecId = RuntimeIdGenerator.generateResourceSpecId();
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
