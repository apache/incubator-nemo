package org.apache.nemo.common.ir.edge.executionproperty;

import org.apache.nemo.common.ir.executionproperty.EdgeExecutionProperty;

import java.util.UUID;

/**
 * Cache ID ExecutionProperty. This property is used for identifying the cached data.
 */
public final class CacheIDProperty extends EdgeExecutionProperty<UUID> {
  /**
   * Constructor.
   *
   * @param value value of the execution property.
   */
  private CacheIDProperty(final UUID value) {
    super(value);
  }

  /**
   * Static method exposing the constructor.
   *
   * @param value value of the new execution property.
   * @return the newly created execution property.
   */
  public static CacheIDProperty of(final UUID value) {
    return new CacheIDProperty(value);
  }
}
