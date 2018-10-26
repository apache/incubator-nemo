package org.apache.nemo.compiler.frontend.spark;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Broadcast variables of Spark.
 */
public final class SparkBroadcastVariables {
  private static final Logger LOG = LoggerFactory.getLogger(SparkBroadcastVariables.class.getName());
  private static final AtomicLong ID_GENERATOR = new AtomicLong(0);
  private static final Map<Serializable, Object> ID_TO_VARIABLE = new HashMap<>();

  private SparkBroadcastVariables() {
  }

  /**
   * @param variable data.
   * @return the id of the variable.
   */
  public static long register(final Object variable) {
    final long id = ID_GENERATOR.getAndIncrement();
    ID_TO_VARIABLE.put(id, variable);
    LOG.info("Registered Spark broadcast variable with id {}", id);
    return id;
  }

  /**
   * @return all the map from ids to variables.
   */
  public static Map<Serializable, Object> getAll() {
    return ID_TO_VARIABLE;
  }
}
