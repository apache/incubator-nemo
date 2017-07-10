package edu.snu.vortex.runtime.master.resource;

import edu.snu.vortex.compiler.ir.attribute.Attribute;

/**
 * Represents the specifications of a resource.
 */
// TODO #249: Clean up Attributes
// We should clean up and add attributes related to resource specification.
public final class ResourceSpecification {
  private final Attribute containerType;
  private final int capacity;
  private final int memory;

  public ResourceSpecification(final Attribute containerType,
                               final int capacity,
                               final int memory) {
    this.containerType = containerType;
    this.capacity = capacity;
    this.memory = memory;
  }

  public Attribute getContainerType() {
    return containerType;
  }

  public int getCapacity() {
    return capacity;
  }

  public int getMemory() {
    return memory;
  }
}


