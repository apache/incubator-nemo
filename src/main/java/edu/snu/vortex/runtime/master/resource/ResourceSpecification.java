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

  /**
   * @return The type of the container.
   */
  public Attribute getContainerType() {
    return containerType;
  }

  /**
   * @return The number of TaskGroups that can be run in this container.
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

  /**
   * @return {@link Builder} for {@link ResourceSpecification}.
   */
  public static Builder newBuilder() {
    return new Builder();
  }

  /**
   * A Builder class for {@link ResourceSpecification}.
   */
  public static final class Builder {
    private Attribute containerType;
    private Integer capacity;
    private Integer memory;

    private Builder() {
    }

    public Builder setContainerType(final String inputContainerType) {
      switch (inputContainerType) {
        case "Transient":
          return setContainerType(Attribute.Transient);
        case "Reserved":
          return setContainerType(Attribute.Reserved);
        case "Compute":
          return setContainerType(Attribute.Compute);
        default:
          throw new IllegalArgumentException("Unknown containerType: " + inputContainerType);
      }
    }

    /**
     * @param inputContainerType the container type
     * @return {@link Builder} object.
     */
    public Builder setContainerType(final Attribute inputContainerType) {
      this.containerType = inputContainerType;
      return this;
    }

    /**
     * @param inputCapacity the number of TaskGroups that can be run in this container
     * @return {@link Builder} object.
     */
    public Builder setCapacity(final int inputCapacity) {
      this.capacity = inputCapacity;
      return this;
    }

    /**
     * @param inputMemory the size of the memory allocated, in megabytes
     * @return {@link Builder} object.
     */
    public Builder setMemory(final int inputMemory) {
      this.memory = inputMemory;
      return this;
    }

    /**
     * @return the {@link ResourceSpecification} object that has been built
     */
    public ResourceSpecification build() {
      assert (containerType != null);
      assert (capacity != null);
      assert (memory != null);
      return new ResourceSpecification(containerType, capacity, memory);
    }
  }
}


