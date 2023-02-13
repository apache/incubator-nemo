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
package org.apache.nemo.runtime.master.resource;

import org.apache.nemo.common.RuntimeIdManager;

import java.util.OptionalDouble;
import java.util.OptionalInt;

/**
 * Represents the specifications of a resource.
 */
// We should clean up and add execution properties related to resource specification.
public final class ResourceSpecification {
  private final String resourceSpecId;
  private final String containerType;
  private final int capacity;
  private final int slot;
  private final int memory;
  private final int poisonSec; // -1 if this resources is not poisoned

  public ResourceSpecification(final String containerType,
                               final int capacity,
                               final int memory) {
    this(containerType, capacity, 0, memory, -1);
  }

  public ResourceSpecification(final String containerType,
                               final int capacity,
                               final int slot,
                               final int memory) {
    this(containerType, capacity, slot, memory, -1);
  }

  public ResourceSpecification(final String containerType,
                               final int capacity,
                               final int slot,
                               final int memory,
                               final int poisonSec) {
    this.resourceSpecId = RuntimeIdManager.generateResourceSpecId();
    this.containerType = containerType;
    this.slot = slot;
    this.capacity = capacity;
    this.memory = memory;
    this.poisonSec = poisonSec;
  }

  @Override
  public String toString() {
    return "[" + containerType + ", capa: " + capacity + ", slot: " + slot + ", mem: " + memory + "]";
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

  public int getSlot() {
    return slot;
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
   *         &gt;= 0 the expected time to failure by poison. (for fault-handling tests)
   */
  public int getPoisonSec() {
    return poisonSec;
  }
}
