package edu.snu.onyx.runtime.master.resource;

import edu.snu.onyx.runtime.common.RuntimeIdGenerator;

/**
 * Represents the specifications of a resource.
 */
// TODO #249: Clean up Execution Properties.
// We should clean up and add execution properties related to resource specification.
public final class ResourceSpecification {
  private final String resourceSpecId;
  private final String containerType;
  private final int capacity;
  private final int memory;

  public ResourceSpecification(final String containerType,
                               final int capacity,
                               final int memory) {
    this.resourceSpecId = RuntimeIdGenerator.generateResourceSpecId();
    this.containerType = containerType;
    this.capacity = capacity;
    this.memory = memory;
  }

  /**
   * @return The type of the container.
   */
  public String getContainerType() {
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

  public String getResourceSpecId() {
    return resourceSpecId;
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
    private String containerType;
    private Integer capacity;
    private Integer memory;

    private Builder() {
    }

    /**
     * @param inputContainerType the container type
     * @return {@link Builder} object.
     */
    public Builder setContainerType(final String inputContainerType) {
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
